"""One annual trend-regime rule shared by 3A, Astra and the deep-analysis page.

Pure functions, no I/O. The same thresholds decide the board gate and the
deep-analysis curve so a stock cannot be "long-term up" in one view and
"down / top warning" in the other. Keep byte-identical in both repositories.
"""
from datetime import date
import math

VERSION = 'trend-regime-v1'
LABELS = {'up': '上行结构', 'up_caution': '高位承压', 'down': '下行结构',
          'range': '震荡结构', 'limited': '年度证据不足'}
KEYS = ('last', 'ma60', 'ma120', 'ma200', 'ma200_slope20_pct', 'annual_return_pct')


def _finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def complete(m):
    return all(_finite((m or {}).get(k)) for k in KEYS)


def base_regime(m):
    if not complete(m):
        return 'limited'
    last, ma60, ma120, ma200 = m['last'], m['ma60'], m['ma120'], m['ma200']
    slope, annual = m['ma200_slope20_pct'], m['annual_return_pct']
    if last > ma60 > ma120 > ma200 and slope > 0 and annual > 0:
        return 'up'
    if last < ma200 and ma60 < ma120 and slope < 0:
        return 'down'
    return 'range'


def volume_top(m):
    m = m or {}
    return bool(_finite(m.get('return1_pct')) and _finite(m.get('volume_ratio20'))
                and m['return1_pct'] <= -3 and m['volume_ratio20'] >= 1.5)


def long_up(m):
    return bool(complete(m) and m['last'] > m['ma120'] and m['ma60'] > m['ma120'] > m['ma200']
                and m['ma200_slope20_pct'] > 0 and m['annual_return_pct'] > 0)


def classify(m, extra_top=False):
    """Return (regime, top_warning) from annual metrics."""
    regime = base_regime(m)
    if regime == 'limited':
        return regime, False
    top = volume_top(m) or bool(extra_top)
    if top and (regime == 'up' or long_up(m)):
        regime = 'up_caution'
    return regime, top


def _months_back(day, months=12):
    year, month = day.year, day.month - months
    while month <= 0:
        month += 12; year -= 1
    for d in (day.day, 30, 29, 28):
        try:
            return date(year, month, d)
        except ValueError:
            continue


def from_series(dates, closes, volumes, basis='screening'):
    """Annual metrics + regime from completed daily bars (ascending)."""
    n = len(closes)
    m = {'basis': basis, 'bars': n, 'last': float(closes[-1]) if n else None}
    if n < 2 or len(dates) != n or len(volumes) != n:
        m.update(regime='limited', top_warning=False, label=LABELS['limited'])
        return m
    for k in (60, 120, 200):
        m[f'ma{k}'] = sum(closes[-k:]) / k if n >= k else None
        m[f'ma{k}_slope20_pct'] = ((sum(closes[-k:]) / k) / (sum(closes[-k - 20:-20]) / k) - 1) * 100 \
            if n >= k + 20 and sum(closes[-k - 20:-20]) > 0 else None
    m['return1_pct'] = (closes[-1] / closes[-2] - 1) * 100 if closes[-2] > 0 else None
    avg = sum(volumes[-21:-1]) / 20 if n >= 21 else 0
    m['volume_ratio20'] = volumes[-1] / avg if avg > 0 else None
    try:
        end = date.fromisoformat(str(dates[-1])[:10]); start = _months_back(end)
        window = [c for d, c in zip(dates, closes) if str(d)[:10] >= start.isoformat()]
    except ValueError:
        window = []
    m['annual_return_pct'] = (window[-1] / window[0] - 1) * 100 if len(window) >= 200 and window[0] > 0 else None
    regime, top = classify(m)
    m.update(regime=regime, top_warning=top, label=LABELS[regime], version=VERSION,
             asof=str(dates[-1])[:10])
    return m
