"""Scored grade tables and separate continuous research tracking."""
from html import escape
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from zoneinfo import ZoneInfo

from grade_focus import MARKETS, GRADES, GRADE_LIMITS as LIMITS

ROOT=Path(__file__).resolve().parent.parent/'ai-daily-report-v2/data'
LISTING_HISTORY_RULE='上次上榜指上一版真实正式榜；同版后台刷新不重复计次，历史名次按当时榜单范围。时间统一为北京时间。'

HORIZON_STYLE='''<style>
.v88-horizon-board{--lane-accent:#2563eb;--lane-bg:#eff6ff}
.v88-horizon-board .v88-horizon-lane{border:1px solid #dbe3ef;border-left:5px solid var(--lane-accent);border-radius:12px;background:#fff;margin:16px 0;overflow:hidden}
.v88-horizon-board .v88-horizon-lane[data-horizon="medium"]{--lane-accent:#b45309;--lane-bg:#fffbeb}
.v88-horizon-board .v88-horizon-lane[data-horizon="long"]{--lane-accent:#7c3aed;--lane-bg:#f5f3ff}
[data-testid="stAppViewContainer"] .v88-horizon-board .v88-horizon-lane>.v88-lane-header,
.v88-horizon-board .v88-horizon-lane>.v88-lane-header{display:flex!important;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:16px;padding:18px 20px!important;margin:0;background:var(--lane-bg);color:#1e293b!important;line-height:1.5!important;font-size:15px!important;list-style:none}
.v88-lane-header::-webkit-details-marker{display:none}
.v88-horizon-board .v88-lane-title{font-size:22px!important;font-weight:800;color:var(--lane-accent)}
.v88-horizon-board .v88-lane-window{font-size:13px!important;color:#64748b;margin-left:12px;font-weight:400;white-space:nowrap}
.v88-horizon-board .v88-lane-markets{display:flex;flex-wrap:wrap;gap:8px 16px;margin-top:5px;font-size:14px!important;color:#475569;font-weight:400}
.v88-horizon-board .v88-lane-result{display:flex;align-items:center;gap:18px;flex-wrap:wrap}
.v88-horizon-board .v88-lane-count{font-size:15px!important;font-weight:600;white-space:nowrap;color:var(--lane-accent)}
.v88-horizon-board .v88-lane-count strong{font-size:32px!important;font-weight:800;line-height:1.1;font-variant-numeric:tabular-nums;margin:0 5px}
.v88-horizon-board .v88-lane-grades{display:flex;gap:6px;align-items:center;flex-wrap:wrap;font-size:12px!important;color:#64748b;font-weight:400}
.v88-horizon-board .v88-lane-grade{padding:3px 8px;background:#fff;border:1px solid #e2e8f0;border-radius:6px;white-space:nowrap;color:#334155}
.v88-horizon-board .v88-lane-grade[data-grade="3A"][data-count]:not([data-count="0"]){background:#f0fdf4;color:#166534;border-color:#bbf7d0}
.v88-horizon-board .v88-lane-grade[data-grade="2A"][data-count]:not([data-count="0"]){background:#fffbeb;color:#92400e;border-color:#fde68a}
.v88-horizon-board .v88-lane-toggle{font-size:12px!important;color:#475569;font-weight:500;white-space:nowrap}
.v88-horizon-board .v88-lane-toggle:after{content:'查看名单 ▾'}
.v88-horizon-board details[open]>.v88-lane-header .v88-lane-toggle:after{content:'收起名单 ▴'}
.v88-horizon-board .v88-lane-header:focus-visible{outline:3px solid var(--lane-accent);outline-offset:-3px}
.v88-horizon-board .v88-lane-body{padding:12px 16px}
.v88-horizon-board .v88-lane-empty{font-size:13px!important;color:#64748b;padding:0;margin:0 0 6px}
.v88-horizon-board .v88-lane-notes{font-size:12px;color:#64748b}
@media(max-width:600px){
[data-testid="stAppViewContainer"] .v88-horizon-board .v88-horizon-lane>.v88-lane-header,
.v88-horizon-board .v88-horizon-lane>.v88-lane-header{padding:14px 12px!important;gap:10px}
.v88-horizon-board .v88-lane-result{width:100%;justify-content:space-between;gap:8px}
.v88-horizon-board .v88-lane-title{font-size:20px!important}
.v88-horizon-board .v88-lane-count strong{font-size:28px!important}
.v88-horizon-board .v88-lane-body{padding:10px 8px}
}
</style>'''


def _horizon_shell(horizon, counts, content):
    """Counts come from the same validated rows as the table, including zero."""
    from grade_focus import HORIZON_LABELS
    total=sum(v['total'] for v in counts.values())
    title,window=HORIZON_LABELS[horizon].split(' · ',1)
    markets=''.join(f'<span data-watch-market="{escape(m)}">{escape(m)} {counts[m]["total"]}只</span>' for m in MARKETS)
    grades=''.join(f'<span class="v88-lane-grade" data-grade="{g}" data-count="{sum(v[g] for v in counts.values())}">{g} · {sum(v[g] for v in counts.values())}只</span>' for g in GRADES)
    header=(f'<span><span class="v88-lane-title">{title}</span><span class="v88-lane-window">{window} · 各市场Top3</span>'
            f'<span class="v88-lane-markets">{markets}</span></span>'
            f'<span class="v88-lane-result"><span class="v88-lane-count">入选 <strong>{total}</strong> 只</span>'
            f'<span class="v88-lane-grades">实际评级 {grades}</span>'
            +('<span class="v88-lane-toggle" aria-hidden="true"></span>' if total else '')+'</span>')
    tag='details' if total else 'section'
    head_tag='summary' if total else 'div'
    opened=' open' if total and horizon=='short' else ''
    return (f'<{tag} class="v88-horizon-lane" data-horizon="{horizon}" data-selected-count="{total}"{opened}>'
            f'<{head_tag} class="v88-lane-header">{header}</{head_tag}>'
            f'<div class="v88-lane-body">{content}</div></{tag}>')


def _listing_history_html(row):
    """Display a verified publication receipt, never infer history from a rank."""
    missing='<div class="v88-listing-history" style="font-size:11px!important;color:#64748b!important">上次上榜：暂无可核验历史</div>'
    history=row.get('listing_history')
    if (not isinstance(history,dict) or history.get('status')!='verified'
            or history.get('rank_scope')!='同市场同评级正式榜'
            or history.get('timezone')!='Asia/Shanghai'):return missing
    previous=history.get('previous')
    if not isinstance(previous,dict):return missing
    market=previous.get('market');tier=previous.get('tier');rank=previous.get('rank')
    listed_at=previous.get('listed_at');key=previous.get('publication_key')
    if (market not in MARKETS or tier not in GRADES or type(rank) is not int or rank<1
            or not isinstance(key,str) or not key.strip() or not isinstance(listed_at,str)):
        return missing
    try:
        at=datetime.fromisoformat(listed_at)
        if at.tzinfo is None or at.utcoffset() is None:return missing
        at=at.astimezone(ZoneInfo('Asia/Shanghai'))
    except (ValueError,TypeError,OverflowError):return missing
    lane={'short':'短期 · ','medium':'中期 · ','long':'长期 · '}.get(previous.get('horizon'),'原混合榜 · ')
    rank_text=(lane+f'{market} Top{rank} · {tier}' if previous.get('rank_scope')=='同周期同市场Top榜' else lane+f'Top{rank} · {market}{tier}' if previous.get('rank_scope')=='同周期三市场Top榜' else lane+f'{market}{tier} 第{rank}名')
    title=escape(f'{at:%Y-%m-%d %H:%M:%S} 北京时间（BJT） · {rank_text}',quote=True)
    return (f'<div class="v88-listing-history" style="font-size:11px!important;color:#64748b!important" title="{title}">'
            +f'上次上榜 {at:%m-%d %H:%M}<br>'+escape(rank_text)+'</div>')


def _scored(row):
    value=row.get('audit_score')
    return row.get('tier') in GRADES and isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and 0<=value<=100


def read(root=ROOT):
    try:return json.loads((Path(root)/'persistent_watchlist_pub.json').read_text(encoding='utf-8'))
    except (OSError,ValueError):return {}


def html(doc,selection,*,code=None,view='period',horizon=None):
    from grade_card import _td,_tbl,stock_link,_flag
    from grade_focus import canonical
    from stock_profile_view import html as profile_html, load as profiles_load, display_name
    from scorecard_html import gpt_html, books_html, profit_html
    from entry_opportunity import html as entry_html, price_html, assess as entry_assess
    from review_display import current_scorecard
    from nontechnical_reason_ui import load_index, for_code, html as business_html, action_html, has_support
    esc=lambda v:escape(str(v if v is not None else '—'))
    if not doc:return ''
    if view=='current' and horizon is None:
        from grade_focus import HORIZONS
        return ''.join(html(doc,selection,code=code,view=view,horizon=lane) for lane in HORIZONS)

    business_index=load_index(ROOT.parent)

    matches=doc.get('factpack_id')==selection.get('factpack_id') and doc.get('source_generated_at')==selection.get('generated_at')
    try:
        matches=matches and 0 <= (datetime.now(timezone.utc)-datetime.fromisoformat(doc['generated_at'])).total_seconds() <= 900
    except (KeyError,ValueError,TypeError):
        matches=False
    profiles=profiles_load(ROOT/'stock_profiles_pub.json')
    if view=='current':
        source=[r for r in (doc.get('current_focus') or {}).get('rows',[]) if _scored(r) and (r.get('horizon') or (r.get('trade_plan') or {}).get('horizon'))==horizon]
        source.sort(key=lambda r:(MARKETS.index(r['market']) if r.get('market') in MARKETS else 9,r.get('watch_rank') if type(r.get('watch_rank')) is int else 999999,canonical(r.get('code'))))
        # A stale producer must not put unreviewed placeholders or unlimited
        # lists into a formal grade table.
        limited=[];seen={}
        for row in source:
            key=row.get('market');seen[key]=seen.get(key,0)+1
            if seen[key]<=LIMITS[row['tier']]:limited.append(row)
        source=limited
    elif view=='tracking':
        from grade_focus import ranked_reserves
        source=ranked_reserves((doc.get('tracking') or {}).get('rows',[]))
    else:source=doc.get('rows',[])
    rows=[r for r in source if code is None or canonical(r['code'])==canonical(code)]
    if view not in ('current', 'tracking') and code is None:
        from display_limits import market_top
        rows=market_top(rows)
    if not rows and (view!='current' or code is not None):return ''
    live_focus={}
    central_by_code={}
    for bucket in ('observations','conditional','preparations','blocked_3a','recommendations','pending','excluded'):
        for item in selection.get(bucket,[]):
            central_by_code.setdefault(canonical(item.get('code')),[]).append(item)
    if view=='current':
        from grade_focus import build as focus_build
        central_rows=[r for b in ('observations','conditional','preparations','blocked_3a','recommendations')
                      for r in selection.get(b,[]) if r.get('tier') in GRADES]
        live_focus=focus_build([{**r,'scorecard':current_scorecard(selection,r)} for r in central_rows])['records']
    pool_note=''
    if view=='current':
        lane_rows=[r for candidates in central_by_code.values() for r in candidates
                   if (r.get('horizon') or (r.get('trade_plan') or {}).get('horizon'))==horizon]
        verified=[r for r in live_focus.values() if r.get('horizon')==horizon and r.get('metrics')]
        failed=sum(r.get('tier')=='0A' or r.get('state') in ('REJECTED','EXCLUDED') for r in lane_rows)
        waiting=max(0,len(lane_rows)-len(verified)-failed)
        blocked=sum(not (r.get('entry_opportunity') or {}).get('focus_eligible') for r in verified)
        pool_note=(f'<div class="v88-horizon-status" style="font-size:12px;color:#475569">'
            f'本周期中央档案 {len(lane_rows)}只 → 当前有效评级 {len(verified)}只；'
            f'缺证/待审 {waiting} · 未授级/排除 {failed} · 已评级但周内入场未通过 {blocked}。'
            '缺少本周期证据时留空，不将另一周期分数搬入。</div>')
    shown_entry_ready=0
    body='';week_archives='';market_bodies={m:'' for m in MARKETS}
    counts={market:{'total':0,'graded':0,'paused':0,'awaiting':0,'stale':0,**{tier:0 for tier in GRADES}} for market in MARKETS}
    for r in rows:
        p=r['trade_plan'];c=r['code'];tier=r.get('tier');score=r.get('audit_score')
        paused=str(r.get('seat_kind','')).startswith('暂停研究')
        label=({'3A':'🟢 ','2A':'🟠 ','1A':'🔵 '}.get(tier,'')+tier+'价值跟踪') if tier in ('1A','2A','3A') else (r['seat_kind'] if paused else '新候选·待双审')
        current = matches
        candidates=central_by_code.get(canonical(c),[])
        central=candidates[0] if len(candidates)==1 else None
        card=current_scorecard(selection,central) if central else {}
        if tier:
            from investment_maturity import assess
            value=assess(card,(central or {}).get('horizon'),(central or {}).get('trade_plan') or {})
            current=(current and value.get('tier')==tier and value.get('value_confirmed') is True
                     and score==card.get('total') and p==(central or {}).get('trade_plan'))
            if view=='current':
                expected=live_focus.get(canonical(c)) or {}
                current=(current and expected.get('selected') is True
                    and type(r.get('watch_rank')) is int and r['watch_rank']==expected.get('rank')
                    and r.get('central_rank')==expected.get('central_rank')
                    and r.get('focus_rank')==expected.get('rank')
                    and r.get('focus_eligible') is bool((expected.get('entry_opportunity') or {}).get('focus_eligible')))
            if not current:label='原'+tier+'·复核中'
        elif paused:
            # A retained identity is not approval. Read rejected/pending rows
            # again as well: an unchanged document hash cannot validate its
            # copied score, changed contract, or an expired review.
            complete=bool((card.get('gpt') or {}).get('current') and (card.get('gpt') or {}).get('complete'))
            entry=r.get('entry_opportunity') or {}
            current=bool(current and central and r.get('scorecard')==card
                and score==(card.get('total') if complete else None)
                and r.get('review_complete') is complete
                and p==central.get('trade_plan') and r.get('horizon')==central.get('horizon')
                and r.get('source_asof')==central.get('factpack_asof')
                and r.get('executable') is False and r.get('focus_eligible') is False
                and entry.get('executable') is False and entry.get('focus_eligible') is False)
            label='⏸ 持续跟踪·暂停' if current else '⏸ 持续跟踪·证据待更新'
        book=r.get('book_checks') or []
        book_text=books_html(r.get('scorecard') or {}) if not book else (
            f'<div>书理 {sum(x.get("ok") is True for x in book)}/{len(book)}项通过</div><details><summary>逐项书理</summary>'
            +'<br>'.join(esc(x['label'])+'：'+('通过' if x.get('ok') is True else '待补证/未通过') for x in book)+'</details>')
        if current and tier:book_text=books_html(card)
        elif not current:book_text='<small>原书理快照 · 待重核</small>'+book_text
        review_card=card if current and (tier or paused) else r.get('scorecard') or {}
        review_html=gpt_html(review_card) if (not paused or current) and (tier or r.get('review_complete')) else '当前双审证据待更新；未授级'
        if not current:review_html='<small>原GPT审核快照 · 待重核</small>'+review_html
        entry=(live_focus.get(canonical(c)) or {}).get('entry_opportunity') if view=='current' and current else r.get('entry_opportunity')
        if central and current and tier:entry=entry_assess({**central,'scorecard':card})
        action=entry_html(entry or {}) if tier else '<b>候选研究·不可执行</b><div>补齐GPT双审及量价触发；不是1A/2A/3A。</div>'
        if paused:action='<b>⏸ 暂停研究·不可执行</b><details><summary>暂停原因与原评级</summary>'+(('原'+esc(r['previous_tier'])+' → 暂停研究<br>') if r.get('previous_tier') else '')+esc(r['reason'])+'</details>'
        if not current:action='<b>当前证据待更新·不可执行</b><div>下列为原研究合同，等待新一轮同源核对。</div>'
        business=for_code(c,business_index,r)
        action=action_html(action,business)
        row_class='v88-watch-history-row' if view=='history' else 'v88-watch-tracking-row' if view=='tracking' else 'v88-watch-row'
        attention=bool(current and (view!='current' or (live_focus.get(canonical(c)) or {}).get('selected') is True))
        if view=='current' and not attention:
            row_class='v88-week-archive-row'
        active_in_view=view!='current' or attention
        shown_entry_ready+=bool(attention and (entry or {}).get('focus_eligible') and has_support(business))
        tally=counts.setdefault(r['market'],{'total':0,'graded':0,'paused':0,'awaiting':0,'stale':0,**{t:0 for t in GRADES}})
        tally['total']+=int(active_in_view);tally['graded']+=bool(active_in_view and current and tier in ('1A','2A','3A'))
        if active_in_view and current and tier in GRADES:tally[tier]+=1
        tally['paused']+=paused;tally['awaiting']+=bool(not paused and not tier);tally['stale']+=not current
        published_rank=r.get('watch_rank')
        valid_rank=type(published_rank) is int and 1<=published_rank<=LIMITS.get(tier,0)
        rank_label=((f'{esc(r["market"])} {esc(tier)} '+('' if current else '原榜')+f'本市场Top{published_rank}'
                     if valid_rank else f'{esc(r["market"])} {esc(tier)} 名次待核') if view=='current'
                    else f'{esc(r["market"])} '+('候补 Top' if view=='tracking' else '跟踪 ')+esc(published_rank))
        weekly=('<br><small>📌 本周主观察 · 与上方周度同股同分</small>'
                if view=='current' and attention and (entry or {}).get('focus_eligible') is True and r.get('weekly_link') is True else '')
        anchor=f' id="v88-watch-{esc(canonical(c))}"' if view=='current' else f' id="v88-tracking-{esc(canonical(c))}"' if view=='tracking' else ''
        from grade_focus import research_window
        window=research_window(r)
        contract_days=(p.get('profit_inputs') or {}).get('max_calendar_days')
        window_text=((esc(window['label'])+'<details style="font-size:11px"><summary>研究窗口 / 原合同覆盖</summary>'
                      +f"原合同最长 {esc(contract_days)}自然日；具体退出日见止盈栏。<br>"
                      +esc(window['extension_status'])+'</details>') if window else '研究窗口待核')
        row_html=(f'<tr{anchor} class="{row_class}" data-code="{esc(c)}" data-market="{esc(r["market"])}" data-tier="{esc(tier or "none")}" data-paused="{str(paused).lower()}" data-continuity="{str(r.get("continuity") is True).lower()}" data-current="{str(current).lower()}">'
               +_td('<b>'+stock_link(display_name(r['name'],c,profiles),c,_flag(c,r['market']))+'</b><br>'+esc(c)+profile_html(c,profiles))
               +_td(rank_label+f'<br><b>{esc(label)}</b><br>'
                    +(f'{"加权审核分" if current and review_card.get("score_policy") else "审核分" if current else "原审核分"} {score:g}/100' if score is not None else '审核分：证据待更新' if paused else '审核分：尚未完成双审')
                    +weekly+_listing_history_html(r))
               +_td(f'<b>{esc(p.get("last"))}</b><br>{esc(str(r.get("source_asof") or "")[:10])}'
                    +f'<br><small>{esc(str(r.get("source_asof") or "")[11:16])} 源时区</small>')
               +_td(action)
               +_td(price_html(entry or {},p))
               +_td(profit_html({'trade_plan':p,'central_trade_plan':p,'horizon':r['horizon']}))
               +_td('<b>'+esc(p.get('stop'))+'</b><details><summary>失效条件</summary>'+esc(p.get('invalidation'))+'</details>')
               +_td(window_text)
               +_td(review_html)
               +_td(book_text)
               +_td(business_html(business)+'🔎 本期持续跟踪'+'<details><summary>依据与升降级条件</summary>'+esc(r['reason'])+'<br>'+esc('；'.join(r.get('gaps') or []))
                    +'<br>暂不宜开仓仍保留原档案；升降级与实际行情见下方演变监控。</details>')+'</tr>')
        if view=='current' and not attention:week_archives+=row_html
        elif view=='current':market_bodies[r['market']]+=row_html
        else:body+=row_html
    title=(f'<div class="v88-persistent-watch-summary" style="font-size:13px;font-weight:600;margin:8px 0">'
           +('按审核分精选' if view=='current' else '候补跟踪与补审（原评级保留）' if view=='tracking' else '本期固定跟踪档案')+f' · {sum(v["total"] for v in counts.values())}只</div>')
    note='<details style="font-size:11px;color:#64748b"><summary>ℹ️ 跟踪名单与评级说明</summary>本期固定跟踪，显示中央最新评级与分数；跟踪序号不代表当前质量排名。降级明确暂停研究、保留原合同。3A表示审核等级；短、中、长期各自推荐，不把周期当等级。</details>'
    if view=='current':
        from grade_focus import RULE
        note='<div class="v88-lane-notes">'+esc(RULE)+'<br>'+LISTING_HISTORY_RULE+'</div>'
    elif view=='tracking':
        market_note=' · '.join(f'<span data-tracking-market="{esc(m)}">{esc(m)} {v["total"]}只（暂停{v["paused"]} / 待双审{v["awaiting"]}）</span>' for m,v in counts.items())
        note='<div style="font-size:11px;color:#64748b">'+market_note+'</div><details style="font-size:11px;color:#64748b"><summary>ℹ️ 跟踪与恢复条件</summary>候补按同市场审核分降序，最多各5只；有真实分数的排前，未审排后。已完成的真实审核显示分数，尚未完成的审核不填造分数。补齐证据并通过当前双审后，按正式评级与分数进入上方对应榜单。</details>'
    note=note.replace('</details>','<br>'+LISTING_HISTORY_RULE+'</details>')
    changes=doc.get('recent_seat_changes') or []
    history=''
    if code is None and changes and view not in ('current','tracking'):
        history='<div style="font-size:11px;color:#64748b">后台保留 '+str(len(changes))+' 条固定席位变更；按名称或代码检索个股可查看完整档案。</div>'
    archive=(f"<details class='v88-week-archives' style='font-size:11px'><summary>榜单待更新 · 原评级跟踪</summary>{_tbl(week_archives)}</details>" if week_archives else '')
    if view=='current':
        for market in MARKETS:
            amount=counts[market]['total']
            body+=f"<tr class='v88-market-group' data-market-group='{esc(market)}'><td colspan='11' style='padding:9px;background:#eaf2ff;font-size:14px;font-weight:700'>{esc(market)} · 入选 {amount} 只"+(' / 最多3只' if amount else ' · 暂无当前有效入选')+'</td></tr>'
            body+=market_bodies[market]
        total=sum(v['total'] for v in counts.values())
        evidence='<details class="v88-lane-notes"><summary>查看审核缺口与排序说明</summary>'+pool_note+note+'</details>'
        content=((f'<div class="v88-lane-notes">按审核分排序 · 经营支持＋周内入场条件通过 {shown_entry_ready}只</div>'+evidence+_tbl(body)) if total else
                 '<div class="v88-lane-empty">暂无当前有效入选；补证复审通过后更新。</div>'+evidence)
        return HORIZON_STYLE+'<div class="v88-horizon-board">'+_horizon_shell(horizon,counts,content+archive)+'</div>'
    content=title+pool_note+note+_tbl(body)+archive+history
    if view=='tracking':return "<details class='v88-research-reserves'><summary>🔎 候补跟踪与补审 · "+str(len(rows))+"只 · 展开原因与原合同</summary>"+content+"</details>"
    return content
