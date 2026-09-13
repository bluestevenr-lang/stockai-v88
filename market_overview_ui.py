"""Compact market header; rule scores are never displayed as probabilities."""
from html import escape
import math


def cells(snapshot):
    result = []
    for market, flag in (('美股', '🇺🇸'), ('A股', '🇨🇳'), ('港股', '🇭🇰')):
        row = (snapshot.get('markets') or {}).get(market) or {}
        layer = row.get('l3') or {}
        pairs = layer.get('probs') or []
        values = {p[0]:p[1] for p in pairs if isinstance(p, (list, tuple)) and len(p) == 2}
        score = values.get('2周')
        valid = type(score) in (int, float) and math.isfinite(score) and 0 <= score <= 100
        # Same direction bands as v88_decision_core.horizon_rules.
        icon, label, color = (('↑', '偏强', '#15803d') if valid and score >= 59 else
                              ('↓', '偏弱', '#b45309') if valid and score <= 41 else
                              ('↔', '震荡', '#64748b') if valid else ('○', '缺证', '#94a3b8'))
        indices = row.get('indices') or []
        # Scores belong to the named L3 index, not whichever index is first.
        index = next((v for v in indices if v.get('name') == layer.get('name')), {}) if layer.get('name') else (indices or [{}])[0]
        name = escape(str(layer.get('name') or index.get('name') or market))
        last_raw = index.get('last'); last = escape(str(last_raw if type(last_raw) in (int,float) and math.isfinite(last_raw) and last_raw > 0 else '—'))
        value = f'{score:g}/100' if valid else '—'
        mood = str((row.get('temperature') or {}).get('label') or '')
        if any(word in mood for word in ('买','卖','加仓','减仓','满仓','跟进','躲')):
            mood = '情绪标签待核'
        clock = '指数行情 '+str(index.get('source_asof') or '日期未记录')+'；方向分日期 '+str(layer.get('source_asof') or '未单独记录')
        result.append(f"<span class='v88-market-item' style='display:inline-block;margin-left:9px'>{flag}<b>{name}</b> {last} "
            f"<span style='color:{color}' title='{escape(clock, quote=True)}；过去约10个交易日窗口的量价规则分，非涨幅、非上涨概率或实测胜率'>{icon} 过去2周档方向 {value}·{label}</span>"
            + (f" <span style='color:#64748b'>{escape(mood)}</span>" if mood else '') + '</span>')
    return ''.join(result)


def legend():
    return ("<span class='v88-market-legend' style='font-size:10px;color:#64748b'>"
            "ⓘ 过去窗口方向分≠涨幅/胜率 · ↑偏强 ≥59　↔震荡 42–58　↓偏弱 ≤41　○缺证</span>")
