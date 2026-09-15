"""Original eleven-column list for weekly research, with no grade authority."""
from datetime import datetime, timezone, timedelta
from html import escape
from collections import Counter
from zoneinfo import ZoneInfo

RANKING='central-five-session-entry-with-weekly-eligibility-v6'


def html(doc,selection,now=None,detail=False,watchlist=None):
    if not doc or doc.get('version')!='weekly-candidates-v1':return ''
    from grade_card import _tbl,_td,_TH_IN,stock_link,_flag,_market_rows
    from stock_profile_view import html as profile_html
    from scorecard_html import gpt_html,books_html
    from stock_reference import reference, compare, canonical
    from review_display import current_scorecard
    from investment_maturity import assess
    central_all=[r for b in ('recommendations','preparations','blocked_3a','conditional','observations','pending','excluded') for r in selection.get(b,[])]
    central_counts=Counter(canonical(r.get('code')) for r in central_all)
    central_rows={canonical(r.get('code')):r for r in central_all if central_counts[canonical(r.get('code'))]==1}
    now=now or datetime.now(timezone.utc)
    try:
        age=(now-datetime.fromisoformat(doc['generated_at'])).total_seconds()/3600
        fresh=0<=age<=(72 if now.weekday()>=5 else 30)
    except (TypeError,ValueError,KeyError):fresh=False
    fresh=bool(fresh and doc.get('factpack_id')==selection.get('factpack_id')
               and doc.get('central_generated_at')==selection.get('generated_at'))
    day=now.astimezone(ZoneInfo('Asia/Shanghai')).date()
    monday=day-timedelta(days=day.weekday())
    if day.weekday()>=5:monday+=timedelta(days=7)
    week=doc.get('week') or {}
    fresh=bool(fresh and week.get('start')==monday.isoformat()
               and week.get('end')==(monday+timedelta(days=4)).isoformat())
    import grade_focus
    live_focus=grade_focus.build([{**r,'scorecard':current_scorecard(selection,r,now)} for r in central_all if r.get('tier') in grade_focus.GRADES], now=now)
    esc=lambda x:escape(str(x if x is not None else '—'))
    num=lambda x:esc(f'{x:.2f}' if isinstance(x,(int,float)) else '—')
    px=lambda x:esc(f'{x:.4f}'.rstrip('0').rstrip('.') if isinstance(x,(int,float)) else '—')
    span=lambda xs:' ～ '.join(px(x) for x in xs) if isinstance(xs,list) and len(xs)==2 else '未核实'
    rows=[];archived_rows=[];live_formal=0;live_slots={}
    for r in _market_rows(doc.get('rows',[])):
        current=fresh and r.get('eligible') is True
        cross_errors=[]
        central=central_rows.get(canonical(r.get('code')))
        if r.get('central_tier') in ('1A','2A','3A'):
            limit=grade_focus.GRADE_LIMITS[r['central_tier']]
            if (doc.get('policy') or {}).get('ranking')!=RANKING:
                cross_errors.append('周度尚未按当前正式榜分数及分档名额规则重核')
            if not central or not r.get('master_ref'):
                cross_errors.append('缺少同版中央引用，周度记录待刷新')
            else:
                cross_errors.extend(compare(reference(selection,central),r)['errors'])
                card=current_scorecard(selection,central,now)
                if assess(card,central.get('horizon'),central.get('trade_plan'))['tier'] != r['central_tier']:
                    cross_errors.append('中央当前审核已失效或评级变化')
                expected_focus=live_focus['records'].get(canonical(r.get('code'))) or {}
                quality_rank=expected_focus.get('central_rank')
                if not (expected_focus.get('selected') is True and type(expected_focus.get('rank')) is int and 1<=expected_focus['rank']<=limit):
                    cross_errors.append('超出当前正式榜本档名额，不能进入周度重点')
                entry_rank=expected_focus.get('rank')
                if not (type(entry_rank) is int and 1<=entry_rank<=limit):
                    cross_errors.append('周度进场顺位不在本档正式名额内')
                if not (expected_focus.get('entry_opportunity') or {}).get('focus_eligible'):
                    cross_errors.append(expected_focus.get('reason') or '进场可行性待复核')
                if (doc.get('master_focus_version')!=grade_focus.VERSION or
                    any((r.get('master_focus') or {}).get(k)!=expected_focus.get(k) for k in ('rank','central_rank','selected','tier','market','sort_key'))):
                    cross_errors.append('周度排序未引用当前中央顺位')
            if watchlist is not None:
                linked=(r.get('master_watchlist') or {})
                try:
                    watch_age=(now-datetime.fromisoformat(watchlist['generated_at'])).total_seconds()
                    watch_current=(0<=watch_age<=900
                        and watchlist.get('factpack_id')==selection.get('factpack_id')
                        and watchlist.get('source_generated_at')==selection.get('generated_at'))
                except (KeyError,ValueError,TypeError):
                    watch_current=False
                seats=[x for x in (watchlist.get('current_focus') or {}).get('rows',[])
                       if canonical(x.get('code'))==canonical(r.get('code'))]
                seat=seats[0] if len(seats)==1 else {}
                seat_matches=bool(central and seat and linked.get('current') is True
                    and type(linked.get('watch_rank')) is int and 1<=linked['watch_rank']<=limit
                    and linked.get('watch_rank')==(live_focus['records'].get(canonical(r.get('code'))) or {}).get('rank')
                    and linked.get('watch_rank')==seat.get('watch_rank')
                    and linked.get('seat_kind')==seat.get('seat_kind')
                    and seat.get('tier')==r.get('central_tier')
                    and seat.get('audit_score')==r.get('audit_score')
                    and seat.get('trade_plan')==central.get('trade_plan')
                    and seat.get('factpack_id')==selection.get('factpack_id')
                    and seat.get('source_asof')==central.get('factpack_asof')
                    and (seat.get('entry_opportunity') or {}).get('focus_eligible') is True)
                if not watch_current or not seat_matches:
                    cross_errors.append('周度与当前主榜席位未形成同版关联，等待同步核对')
            current=current and not cross_errors
        else:
            cross_errors.append('尚未获得当前中央评级，保留线索等待双审')
            current=False
        if current:
            live_slots[r.get('market')]=live_slots.get(r.get('market'),0)+1
            live_formal+=int(r.get('central_tier') in ('2A','3A'))
        plan=r.get('trade_plan') or {};pc=r.get('profit_contract') or {};h=r.get('history') or {};rot=r.get('rotation') or {}
        gaps=r.get('gaps') or []
        audit=num(r.get('audit_score'))+'/100' if r.get('audit_score') is not None else '尚无可用审核分'
        grade=esc(r.get('central_tier') or '尚未授级')
        ranks=r.get('master_focus') or {}
        association=(f"<a href='#v88-watch-{esc(canonical(r.get('code')))}'>短期Top{esc((r.get('master_watchlist') or {}).get('watch_rank'))} · {grade}同股同分</a>"
                     if current and watchlist is not None else
                     ('主榜关联待核对' if watchlist is not None else '主榜位置：当前视图未载入'))
        score=(f"<b style='color:#0369a1'>{esc(r.get('state')) if current else '准状态·待当日重核'}</b>"
               f"<br>{'当前' if current else '原'}评级 {grade} · 加权审核 {audit}"
               +(f"<br>本市场{grade}审核分第{esc(ranks.get('central_rank'))}名" if current else
                 '<br>当前不参与周度名次；原审核与合同继续留档')
               +
               f"<br>{association} · {len(gaps)}项缺口"
               f"<details><summary>周度辅助指标</summary>筛选 {num(r.get('screen_score'))}/100；用于轮动与位置研究，不改变中央排序。</details>")
        parts='；'.join(esc(k)+' '+num(v) for k,v in (r.get('score_parts') or {}).items())
        exclusions=''.join('<div>'+stock_link(x.get('name'),x['code'])+f" · 审核 {num(x.get('audit_score'))} · 同档第{esc(x.get('rank'))}名："+
                          esc(x.get('reason'))+'；距原区间 '+num(x.get('entry_distance_pct'))+'%</div>' for x in _market_rows(sorted(r.get('higher_rank_exclusions',[]),key=lambda x:x.get('rank') or 999)))
        why=(f"<b>统一主榜＋周度条件</b><br>{esc(r.get('ranking_reason'))}<br>"
             +(f"<span style='color:#b45309'>{esc('；'.join(cross_errors))}</span><br>" if cross_errors else '')
             +f"<details class='v88-weekly-cross'><summary>为什么周重点与总榜首名不同</summary>{exclusions or ('本股已是本市场通过周度条件的最高中央排序。' if current else '本记录当前未通过周度条件；只保留原证据与合同，不参与本期推荐排序。')}"
             f"<br>仅在当前正式榜内另核流动性、完整行情、原止损、未来5交易日可达价带及确认步骤；周度不插队、不改分。"
             f"<br>同一审核/原合同引用 {esc((r.get('master_ref') or {}).get('reference_id','')[:12])}</details>"
             f"{esc(r.get('continuity'))}<br>"
             f"<details><summary>升级条件与反证（{len(gaps)}项）</summary>"+
             ''.join('<p>'+esc(g['title'])+'：'+esc(g.get('observed'))+'<br>需：'+esc(g.get('condition'))+'</p>' for g in gaps)+
             '</details><details><summary>2A / 3A 升级门槛</summary>'+
             '2A：两份GPT五项各≥15、审核分≥75、至多一项成熟缺口；净空间≥'+num(pc.get('period_thresholds',{}).get('2A'))+'%，净RR≥1.5。'+
             '<br>3A：两份GPT与本周期书理全部通过；净空间≥'+num(pc.get('period_thresholds',{}).get('3A'))+'%，净RR≥2。'+
             '<br>须补齐证据后真实重审；不能手调分数。</details><details><summary>筛选分与退出闭环</summary>'+parts+'<p>'+esc(r.get('failure_rule'))+'</p></details>')
        rotation=(f"走势代理 {esc(rot.get('name'))} · 5日 {num(rot.get('return5_pct'))}%"
                  f"<br>相关 {num(rot.get('correlation'))} · {esc(rot.get('sample_n'))}日（非行业认定）"
                  if rot else '尚无可靠板块走势关联')
        location=(f"<br>{esc(h.get('position_bars'))}日位置 {num(h.get('position252_pct'))}% · "
                  +('低位已企稳' if h.get('low_recovery') else '未形成低位企稳共振')+
                  f"<details><summary>原始位置与轮动证据</summary>{esc(h.get('history_start'))} ～ {esc(h.get('history_end'))}"
                  f" · {esc(h.get('history_bars'))}根；区间低 {num(h.get('low252'))}，高 {num(h.get('high252'))}；"
                  f"5日 {num(h.get('return5_pct'))}%<br>"+
                  (f"<a href='{esc(rot.get('source'))}' target='_blank' rel='noopener'>板块来源</a> · {esc(rot.get('source_asof'))}" if rot else '')+
                  '<br>位置分位不是估值，不称为上市以来最低点。</details>')
        card=(r.get('scorecard') or {}) if current else {}
        book_card=card if card.get('books') else {'books':{'checks':r.get('book_checks',[]),
            'pass_n':sum(c.get('ok') is True for c in r.get('book_checks',[])), 'required':len(r.get('book_checks',[]))}}
        rows.append(f"<tr class='v88-weekly-row' data-code='{esc(r.get('code'))}' data-cross-ok='{str(not cross_errors).lower()}'>"+''.join(_td(v) for v in (
            stock_link(r.get('display_name') or r.get('name'),r.get('code'),_flag(r.get('code'),r.get('market')))+'<br>'+esc(r.get('code'))+profile_html(r.get('code')),
            score,'<b>'+px(r.get('last'))+'</b><br>收盘 '+esc(r.get('source_asof')),
            (__import__('entry_opportunity').html(__import__('entry_opportunity').assess({**(central or {}),'scorecard':card}, now=now))
             + '<br>周度研究·不可执行' if current else '<b>⏸ 历史周度·不可执行</b><br>等待当周同版证据与主榜关联重核'),
            __import__('entry_opportunity').price_html(__import__('entry_opportunity').assess({**(central or {}),'scorecard':card}, now=now) if current else {},plan),
            '<b>'+span(plan.get('take_profit_range'))+'</b><br>原合同净空间 '+num(pc.get('net_upside_pct'))+'%<br>净RR '+num(pc.get('net_reward_risk'))+
              '<br>'+esc(pc.get('holding_sessions'))+'交易日 · 截止 '+esc(pc.get('thesis_deadline')),
            px(plan.get('stop'))+'<br>'+esc(plan.get('invalidation')),
            rotation+location,gpt_html(card),books_html(book_card),why))+'</tr>')
        if not current:
            archived_rows.append(rows.pop())
    week=doc.get('week') or {};slots=doc.get('market_slots') or {}
    formal=live_formal
    caption=' · '.join(esc(m)+' '+str(live_slots.get(m,0))+'/1' for m,s in slots.items())
    histories='<div>完整个股档案可按名称或代码检索。</div>'
    styles=[]
    sector_brief='；'.join(esc(m)+'：'+('、'.join(esc(p['name'])+' '+num(p['chg5d'])+'%（5日）' for p in group) or '缺当前证据')
        for m,group in (doc.get('sector_rotation') or {}).items())
    styles.append('<details><summary>本轮板块轮动证据 · '+
        ' / '.join(esc(m)+' '+str(n)+'个代理' for m,n in (doc.get('context_sources') or {}).items())+
        '</summary>'+sector_brief+'<br>板块/风格ETF代理；有行情缺口的代理不参与排序。</details>')
    for key,title in (('low_recovery_candidates','低位企稳补充筛选'),('rotation_candidates','轮动转强补充筛选')):
        names=[]
        for market,group in (doc.get(key) or {}).items():
            names.append(esc(market)+'：'+('、'.join(stock_link(r.get('display_name') or r.get('name'),r['code'])+
                '（'+num(r.get('history',{}).get('position252_pct'))+'%位置，筛选'+num(r.get('screen_score'))+'）' for r in _market_rows([{**r,'market':r.get('market') or market} for r in group])) or '暂无同条件候选'))
        styles.append('<div><b>'+title+'</b> · '+'；'.join(names)+'</div>')
    archive=("<details class='v88-weekly-archive'><summary>暂停的周度候选 · 保留原合同与解除条件</summary>"
             +_tbl(''.join(archived_rows),_TH_IN)+"</details>") if archived_rows else ''
    if detail:
        return ("<section class='v88-weekly-candidates' style='font-size:12px;margin:8px 0'>"
                +f"<b>周度候选证据 · {esc(week.get('start'))} ～ {esc(week.get('end'))}</b>"
                +("<div>原周度证据已过期或事实包变化，等待当日重核。</div>" if not fresh else "")
                +_tbl(''.join(rows),_TH_IN)+archive+"</section>")
    return ("<section class='v88-weekly-candidates' style='font-size:12px;margin:8px 0'>"
            f"<div style='font-size:13px;font-weight:700'>周度三市场重点候选 · {esc(week.get('start'))} ～ {esc(week.get('end'))}</div>"
            f"<div>{caption} · 当前正式2A/3A {formal}只 · 准状态保留原评级，筛选分不代表胜率</div>"
            +(f"<div style='color:#b45309'>原周度快照已过期或事实包已变化；等待当日重核，保留原跟踪记录。</div>" if not fresh else '')+
            _tbl(''.join(rows),_TH_IN)+archive+''.join(styles)+f'<details><summary>排序规则与跟踪记录</summary><p>沿用中央同档排序与进场可行性；周度再核时效、流动性及原区间距离。未审核发现线索不填补重点名额。辅助指标不覆盖审核分；更换主候选保留原合同及沿革。</p>{histories}</details></section>')
