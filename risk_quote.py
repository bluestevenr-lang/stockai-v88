"""Read-only quote enrichment: signal closes are never overwritten by quotes."""
from datetime import datetime, timezone
from html import escape
from math import isfinite
from zoneinfo import ZoneInfo
from market_symbols import canonical


def attach_quotes(rows, base, now):
    from market_watch import strategy_quotes, quote_fact, stamp
    from free_market_data import code_for
    wanted={canonical(r['code']):r for r in rows}
    found={}
    for market in {r['market'] for r in rows}:
        try: quotes,_=strategy_quotes(base,market,now)
        except (ValueError,KeyError,TypeError,OSError): continue
        for _,raw,receipt in quotes:
            code=canonical(code_for(market,raw))
            if code not in wanted or wanted[code]['market']!=market:continue
            try:
                q=quote_fact(raw,wanted[code],market,now,stamp(receipt['captured_at']))
                found[code]={k:q[k] for k in ('code','market','last','change_pct','quote_asof','source_session','quote_kind')}
                found[code].update(source_sha256=receipt['sha256'],provider=receipt['provider'])
            except (ValueError,KeyError,TypeError,OSError):continue
    for row in rows:
        # This is a display field, independent of signal prices and original stops.
        row['latest_quote']=found.get(canonical(row['code']))
    return rows


def quote_html(row, now=None):
    now=now or datetime.now(timezone.utc)
    esc=lambda v:escape(str(v),quote=True)
    currency={'A股':'人民币','港股':'港元','美股':'美元'}.get(row.get('market'),'币种待核')
    q=row.get('latest_quote') or {}
    try:
        price=float(q['last']);at=datetime.fromisoformat(q['quote_asof'].replace('Z','+00:00'))
        if not isfinite(price) or price<=0 or at.tzinfo is None or at>now:raise ValueError('quote')
        if canonical(q['code'])!=canonical(row['code']) or q['market']!=row['market']:raise ValueError('identity')
        change=float(q['change_pct'])
        if not isfinite(change):raise ValueError('change')
        clock=at.astimezone(ZoneInfo('Asia/Shanghai')).strftime('%m-%d %H:%M:%S')
        label='最近报价 · 非实时' if (now-at).total_seconds()>1200 else '最新报价 · 源延迟未保证'
        return (f'<div class="risk-price"><strong>{price:g}</strong> <span>{currency}</span></div>'
                f'<div class="risk-change">{change:+.2f}%</div>'
                f'<small>{clock} 北京时间<br>{label}<br>信号日线 {esc(row.get("source_asof") or "待核")}</small>')
    except (ValueError,KeyError,TypeError,OverflowError):pass
    try:
        price=float(row['last'])
        if not isfinite(price) or price<=0:raise ValueError('close')
        return (f'<div class="risk-price"><strong>{price:g}</strong> <span>{currency}</span></div>'
                f'<div>最近收盘 · {esc(row.get("source_asof") or row.get("window_end") or "日期待核")}</div>'
                '<small>最新报价待更新 · 非实时</small>')
    except (ValueError,KeyError,TypeError,OverflowError):
        return '<b>现价待更新</b><br><small>无已核报价；不以0代替</small>'
