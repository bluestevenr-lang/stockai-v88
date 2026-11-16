"""3A 入场质量层（唯一口径，已并入 strategy_board 主裁决）。

历史：本模块原为「影子筛选层」，并行生成一份影子 3A 名单只记录不改主榜，
导致同一程序里存在两套 3A 结论（主榜一套、影子一套）。2026-10-20 起：
  · 入场质量判断只在 strategy_board.apply_entry_gate 里执行一次，直接决定 3A 名单；
  · 本模块只提供判断所需的工具函数（大盘环境、板块轮转象限、追高熔断、反弹发现、
    结构盈亏比）——盈亏比统一走 strategy_board_state.opportunity 同一公式；
  · data/shadow_roster.json 仅保留诊断信息（大盘环境、板块象限、深度回撤观察、
    与主榜同源的反弹候选镜像），不再输出任何「影子可买 / 观察」平行裁决。

规则：
  追高熔断：近 52 周高位(≥×0.90)且（偏离MA20>8% / 量比>1.5 / 高位长上影）→ 不入 3A
  盈亏比门：现价至最近上方结构位净空间≥5%且净收益风险比≥2（与主榜 opportunity 同公式）
  环境门：大盘逆风（指数收盘<MA60 且 MA20 较 5 日前下行）→ 买入条件降为重点观察
  板块轮转门：近高位且行业等权象限「短期转弱」→ 买入条件降为重点观察
  反弹救回：探低回收/更高低点/企稳放量满足其二、止损锚于≥1×ATR 的已验证低点、盈亏比门通过
"""
from datetime import datetime, timezone
from pathlib import Path
import bisect
import json

BASE = Path(__file__).resolve().parents[1]
VERSION = 'entry-quality-v3-overheat'

STRETCH_MAX = 6.0      # 偏离 MA20 上限（%），不论是否近高位
MA60_STRETCH_MAX = 10.0  # 偏离 MA60 上限（%）
RET20_MAX = 10.0       # 近 20 日涨幅上限（%）
# 过热门证据（2026-11-03，推荐日志 09-28～11-02 首次入选 456 只，次日开盘买入）：
#   任一命中（MA20≥6% / MA60≥10% / 20日≥10%）10日 绝对胜率20% 中位-5.8% 超额胜率38%；
#   未命中 绝对胜率46% 中位-0.7% 超额胜率67%。3A 单看：命中中位-5.8% vs 未命中+0.4%。
#   旧口径（近52周高位且MA20>8%）同期只命中 8/347，等于不设防。
VR_MAX = 1.5          # 近高位时量比上限

NEAR_HIGH = 0.90      # 现价 ≥ 52 周高 ×0.90 视为「近高位」
ROT_MIN_MEMBERS = 3   # 行业等权象限至少需要的成分股数
DEEP_HI_FRAC = 0.40   # 现价 ≤ 52 周高 ×0.40（即跌破高点 60%）＝深度回撤
# 板块轮转门证据：近高位×板块象限（研究脚本 research/rot.py，同口径时间切分）
# 20 日前向收益两段一致，「短期转弱」（中期强、短期掉头向下＝轮转刚向下）均为最差桶：
#   训练 f20：占优-0.14 改善+0.12 转弱-0.22 落后-0.01；样本外 f20：占优+0.66 改善+0.59 转弱+0.26 落后+0.51
ROT_BLOCK_QUAD = '短期转弱'

ASTRA_MIN_SCORE = 60.0   # Astra 自身入选分线（selection_policy.min_score）
ASTRA_MIN_NET_PCT = 6.0  # Astra 最小净目标空间（min_net_target_pct）
ASTRA_MIN_RR = 2.0       # Astra 最小净盈亏比（min_net_rr）

POLICY = {'version': VERSION, 'stretch_max': STRETCH_MAX, 'ma60_stretch_max': MA60_STRETCH_MAX,
          'ret20_max': RET20_MAX, 'vr_max': VR_MAX,
          'near_high': NEAR_HIGH, 'rot_block_quad': ROT_BLOCK_QUAD, 'deep_hi_frac': DEEP_HI_FRAC,
          'peak_rule': '过热（偏离MA20≥6% / 偏离MA60≥10% / 20日涨幅≥10%，任一）或 近52周高位且量比>1.5/高位长上影 → 不追高，只挂回踩单（追高熔断）',
          'rr_rule': '现价至最近上方结构位：净空间≥5%且净收益风险比≥2（strategy_board_state.opportunity 同一公式），不足不入3A',
          'env_rule': '逆风＝指数收盘<MA60 且 MA20 较 5 日前下行；逆风市场买入条件降为重点观察（反弹可救回）',
          'rotation_rule': '个股近高位且所在行业等权象限为「短期转弱」＝轮转向下不追高，买入条件降为重点观察',
          'deep_value_rule': '现价≤52周高×0.40(跌破60%)仅入「深度回撤观察」；样本外证明其为接飞刀(f20 -0.8%、回撤 -19.8%)，须 Sperandeo 1-2-3 确认趋势反转方可考虑',
          'basis': 'V88-2.0-STEP2-PROPOSAL-2026-10-20.md；轮转门见 research/rot.py；2026-10-20 并入主榜，不再并行影子名单'}

MAIN_INDEX = {'A股': '000001.SS', '港股': '^HSI', '美股': '^GSPC'}


def _finite(x):
    return isinstance(x, (int, float)) and x == x and abs(x) != float('inf')


_IND_CACHE = {}


def load_industry(base=BASE):
    """读取 code→行业 映射（stock_profiles_pub.json），用于现算板块象限。"""
    key = str(base)
    if key in _IND_CACHE:
        return _IND_CACHE[key]
    out = {}
    try:
        doc = json.loads((Path(base) / 'data/stock_profiles_pub.json').read_text('utf-8'))
        for code, rec in (doc.get('records') or {}).items():
            ind = rec.get('industry')
            if ind:
                out[code] = ind
    except Exception:
        out = {}
    _IND_CACHE[key] = out
    return out


def _quad(rs5, rs20):
    """与 research/rot.py、sector_rotation 同口径：短期/中期相对强弱象限。"""
    if rs5 == 0 or rs20 == 0:
        return '中性'
    return ('持续占优' if rs5 > 0 else '短期转弱') if rs20 > 0 else ('短期改善' if rs5 > 0 else '持续落后')


def industry_quadrants(rows, base=BASE):
    """从全体扫描样本按（市场, 行业）等权现算板块象限。

    相对强弱＝行业等权 N 日收益 − 同市场等权 N 日收益；口径与回测 research/rot.py 一致。
    成分股不足 ROT_MIN_MEMBERS 的行业记为「样本不足」。
    """
    ind_map = load_industry(base)
    mkt = {}   # market -> [r5...], [r20...]
    grp = {}   # (market, industry) -> [r5...], [r20...]
    for q in rows:
        e = q.get('structure') or {}
        r5, r20 = e.get('return5_pct'), e.get('return20_pct')
        market = q.get('market')
        ind = ind_map.get(q.get('code'))
        if not (market and ind and _finite(r5) and _finite(r20)):
            continue
        mkt.setdefault(market, [[], []])
        mkt[market][0].append(r5); mkt[market][1].append(r20)
        grp.setdefault((market, ind), [[], []])
        grp[(market, ind)][0].append(r5); grp[(market, ind)][1].append(r20)
    mkt_mean = {k: (sum(v[0]) / len(v[0]), sum(v[1]) / len(v[1])) for k, v in mkt.items() if v[0]}
    out = {}
    for (market, ind), v in grp.items():
        if len(v[0]) < ROT_MIN_MEMBERS or market not in mkt_mean:
            out[(market, ind)] = {'quad': '样本不足', 'n': len(v[0])}
            continue
        ir5, ir20 = sum(v[0]) / len(v[0]), sum(v[1]) / len(v[1])
        mr5, mr20 = mkt_mean[market]
        rs5, rs20 = round(ir5 - mr5, 4), round(ir20 - mr20, 4)
        out[(market, ind)] = {'quad': _quad(rs5, rs20), 'rs5': rs5, 'rs20': rs20, 'n': len(v[0])}
    return out


def near_high(q):
    """现价是否处于 52 周高位（≥ 高点 ×NEAR_HIGH）。"""
    e = q.get('structure') or {}
    last, hi52 = q.get('last'), e.get('high52')
    if not (_finite(last) and _finite(hi52) and hi52 > 0):
        return None
    return last >= hi52 * NEAR_HIGH


def classic_notes(q):
    """统一经典标注层（跨 3A / Astra / 板块 / 个股深度分析口径一致）。

    已验证并作为门槛的项：大盘环境、板块轮转（见 build）。
    仅作标注/确认、未经样本外独立验证的项，一律标明「标注·未独立验证」或「数据待补」，
    不单独否决、不单独升级（总规格第五、十一节：经典因子免检豁免不存在）。
    """
    e = q.get('structure') or {}
    last = q.get('last')
    ma20, ma60 = e.get('ma20'), e.get('ma60')
    hi52 = e.get('high52')
    notes = {}
    # 欧奈尔：仅保留 8% 绝对止损（禁用杯柄/枢纽突破买点）
    if _finite(last):
        notes['oneil_8pct_stop'] = round(last * 0.92, 4)
    # GMMA 近似：现价相对 MA20 / MA60 的位置（趋势资格参考，非买点）
    pos_ma = []
    if _finite(last) and _finite(ma20):
        pos_ma.append('上穿MA20' if last >= ma20 else '下穿MA20')
    if _finite(last) and _finite(ma60):
        pos_ma.append('上穿MA60' if last >= ma60 else '下穿MA60')
    notes['gmma_like'] = '、'.join(pos_ma) if pos_ma else '数据待补'
    if _finite(last) and _finite(hi52) and hi52 > 0:
        notes['off_high_pct'] = round((last / hi52 - 1) * 100, 1)
    # 蜡烛图形态、Sperandeo HH/HL 趋势线需完整日K序列，当前只有快照 → 诚实待补
    notes['candlestick'] = '需日K序列，标注待补'
    notes['sperandeo_123'] = '需摆动高低点序列，标注待补'
    return notes


def deep_value_watch(rows, base=BASE):
    """深度回撤观察清单：现价 ≤ 52 周高 ×DEEP_HI_FRAC（跌破 60%）。

    样本外证据（research/rot.py）：此类为接飞刀（f20 −0.8%、20日回撤 −19.8%），
    朴素企稳触发反而更差。故只作观察、绝不发买入，再入场须 Sperandeo 1-2-3 确认。
    """
    out = []
    for q in rows:
        e = q.get('structure') or {}
        last, hi52 = q.get('last'), e.get('high52')
        if not (_finite(last) and _finite(hi52) and hi52 > 0 and last <= hi52 * DEEP_HI_FRAC):
            continue
        out.append({'code': q.get('code'), 'name': q.get('name'), 'market': q.get('market'),
                    'last': last, 'off_high_pct': round((last / hi52 - 1) * 100, 1),
                    'classic': classic_notes(q),
                    'reentry': '仅观察；须 Sperandeo 1-2-3（破下降趋势线→回踩更高低点→突破最近波段高）确认后再评，朴素企稳不作数',
                    'note': '接飞刀风险：样本外 f20 −0.8%、20日回撤 −19.8%，不发买入'})
    out.sort(key=lambda x: x['off_high_pct'])
    return out


# ───────── 入场质量层（追高熔断 + 反弹发现 + RS 例外）─────────
REBOUND_MIN_FRAC = 0.40   # 反弹候选下限：现价 > 52周高×0.40（非接飞刀）
REBOUND_MAX_FRAC = 0.95   # 反弹候选上限：现价 < 52周高×0.95（非绝对顶，留反弹空间）
HIREV_POS = 0.60          # 探低回收：收盘位于当日振幅上 40%
PEAK_SHADOW_ATR = 1.0     # 高位长上影：上影 > 1×ATR


def _market_r20_means(rows):
    """各市场等权 20 日收益均值，用于相对强度（RS）判定。"""
    agg = {}
    for q in rows:
        e = q.get('structure') or {}
        m = q.get('market'); r20 = e.get('return20_pct')
        if m and _finite(r20):
            agg.setdefault(m, []).append(r20)
    return {m: sum(v) / len(v) for m, v in agg.items() if v}


def structural_stop(q):
    """最近一个距现价≥1×ATR 的已验证低点（5日低优先，否则20日低）；没有则返回 None。"""
    e = q.get('structure') or {}
    last = q.get('last'); atr = e.get('atr14')
    if not (_finite(last) and last > 0 and _finite(atr) and atr > 0):
        return None
    lows = [x for x in (e.get('low5'), e.get('low20')) if _finite(x) and 0 < x <= last - atr]
    return max(lows) if lows else None


def structural_opportunity(q):
    """无价格计划时，用结构止损/结构目标走主榜同一个 opportunity() 公式算当前净空间与盈亏比。

    入场＝现价；止损＝structural_stop；目标上限＝现价+6×ATR（测量移动封顶），
    实际目标由 opportunity 取最近上方结构位（前20日高/60日高/120日高/52周高）。
    """
    from strategy_board_state import opportunity
    e = q.get('structure') or {}
    last = q.get('last'); atr = e.get('atr14')
    stop = structural_stop(q)
    if stop is None:
        return {'passed': False, 'current_net_rr': None, 'gaps': ['缺少距现价≥1×ATR的已验证低点，结构止损待核']}
    row = {'last': last, 'entry_range': [last * 0.9999, last], 'stop_range': [stop, stop],
           'take_profit_range': [last + 6 * atr, last + 6 * atr]}
    return opportunity(row, e, target_basis='cap')


def entry_quality(q, env='neutral', market_r20=0.0):
    """追高熔断 + 反弹发现（威科夫 Spring、Sperandeo 更高低点）。

    追高熔断：过热（偏离MA20≥6%／偏离MA60≥10%／20日涨幅≥10%，任一，不论位置），
      或近 52 周高位且（放量／高位长上影冲高回落）→ peak_block。
    反弹发现（均衡＝满足其二）：回调但非接飞刀，出现 更高低点／探低回收／企稳放量，
      止损锚于≥1×ATR 的已验证低点，且结构盈亏比门通过（净空间≥5%、净收益风险比≥2）；
      逆风仅在 RS 占优且有更高低点时放行（试仓级）。
    """
    e = q.get('structure') or {}
    last = q.get('last'); hi52 = e.get('high52'); ma20 = e.get('ma20'); atr = e.get('atr14')
    low5 = e.get('low5'); low20 = e.get('low20'); vr = e.get('volume_ratio20')
    r5 = e.get('return5_pct'); r20 = e.get('return20_pct')
    dh, dl = q.get('day_high'), q.get('day_low')
    out = {'peak_block': False, 'peak_reasons': [], 'rebound': False, 'rebound_signals': [],
           'level': None, 'rs_strong': bool(_finite(r20) and _finite(market_r20) and r20 > market_r20),
           'entry': None, 'stop': None, 'target': None, 'net_rr': None}
    if not (_finite(last) and _finite(hi52) and hi52 > 0 and _finite(ma20) and ma20 > 0 and _finite(atr) and atr > 0):
        return out
    frac = last / hi52
    near = frac >= NEAR_HIGH
    stretch = (last / ma20 - 1) * 100
    pos = (last - dl) / (dh - dl) if _finite(dh) and _finite(dl) and dh > dl else None
    upper = (dh - last) / atr if _finite(dh) and _finite(dl) and dh > dl else None
    if stretch >= STRETCH_MAX:
        out['peak_reasons'].append(f'偏离MA20 {stretch:+.1f}%（≥{STRETCH_MAX:g}%，涨离均线过远）')
    ma60 = e.get('ma60')
    if _finite(ma60) and ma60 > 0 and (last / ma60 - 1) * 100 >= MA60_STRETCH_MAX:
        out['peak_reasons'].append(f'偏离MA60 {(last / ma60 - 1) * 100:+.1f}%（≥{MA60_STRETCH_MAX:g}%，右侧已走远）')
    if _finite(r20) and r20 >= RET20_MAX:
        out['peak_reasons'].append(f'20日已涨 {r20:+.1f}%（≥{RET20_MAX:g}%，涨幅已兑现）')
    if near:
        if _finite(vr) and vr > VR_MAX:
            out['peak_reasons'].append(f'近高位放量 量比{vr:.2f}（>{VR_MAX:g}）')
        if pos is not None and pos < 0.5 and upper is not None and upper > PEAK_SHADOW_ATR:
            out['peak_reasons'].append('高位长上影·冲高回落（收在当日振幅下半且上影>1×ATR）')
    out['peak_block'] = bool(out['peak_reasons'])
    if REBOUND_MIN_FRAC < frac < REBOUND_MAX_FRAC and not out['peak_block']:
        sig = []
        if _finite(low5) and _finite(low20) and low5 > low20:
            sig.append('更高低点（5日低>20日低）')
        if pos is not None and pos >= HIREV_POS and _finite(dl) and dl > 0 and (last - dl) / last * 100 >= 1.0:
            sig.append('探低回收（收在当日振幅上40%）')
        if last >= ma20 and _finite(vr) and vr >= 1.0 and _finite(r5) and r5 > 0:
            sig.append('企稳放量（站回MA20·量能不缩·5日翻红）')
        out['rebound_signals'] = sig
        if len(sig) >= 2:
            opp = structural_opportunity(q)
            allow = env in ('tailwind', 'neutral') or (
                env == 'headwind' and out['rs_strong'] and '更高低点（5日低>20日低）' in sig)
            if opp.get('passed') and allow:
                out['rebound'] = True
                out['level'] = '试仓' if (env == 'headwind' or len(sig) == 2) else '标准'
                out['entry'] = round(last, 3); out['stop'] = round(opp['stop'], 3)
                out['target'] = round(opp['first_objective'], 3)
                out['net_rr'] = round(opp['current_net_rr'], 2)
                out['net_space_pct'] = round(opp['net_space_pct'], 2)
    return out


def rebound_buys(source_rows, envs):
    """全体扫描样本上发现探低反弹买入候选；逆风市场仅 RS 占优者每市场 1 只试仓级。"""
    means = _market_r20_means(source_rows)
    cand = []
    for q in source_rows:
        m = q.get('market')
        env = (envs.get(m) or {}).get('env', 'neutral')
        eq = entry_quality(q, env, means.get(m, 0.0))
        if eq['rebound']:
            cand.append({'code': q.get('code'), 'name': q.get('name'), 'market': m, 'last': q.get('last'),
                         'level': eq['level'], 'signals': eq['rebound_signals'], 'entry': eq['entry'],
                         'stop': eq['stop'], 'target': eq['target'], 'net_rr': eq['net_rr'],
                         'net_space_pct': eq.get('net_space_pct'), 'rs_strong': eq['rs_strong'],
                         'env': env, 'classic': classic_notes(q)})
    cand.sort(key=lambda x: (-len(x['signals']), -(x['net_rr'] or 0)))
    seen = {}; final = []
    for x in cand:
        if x['env'] == 'headwind':
            if seen.get(x['market'], 0) >= 1:
                continue
            seen[x['market']] = seen.get(x['market'], 0) + 1
        final.append(x)
    return {'candidates': final, 'count': len(final),
            'note': '反弹买入候选（入场质量层）：探低回收／更高低点／企稳放量满足其二，止损锚于≥1×ATR的已验证低点，'
                    '净空间≥5%且净收益风险比≥2（与3A同一公式）；逆风仅RS占优者每市场1只试仓级。非深跌接飞刀；未达3A分数线，不占3A名额。'}


REBOUND_FROM_LOW = 1.03   # 指数离20日低反弹≥3%
ASTRA_POSITION_NOTE = ('Astra＝一个月内回报：与3A/2A/1A同一张周期证据表（horizon_book），只买「市场×当前宽度×形态×分组」'
                       '的规则：前后两半段回测胜率各≥53%（A股低宽度低位企稳20日+10%、缩量回踩前3 20日+8%；美股低宽度低位企稳'
                       '20日+8%、金叉前3 10日+6%）。没命中就只观察（回测胜率不足50%，不可买），非收益保证。')


FADE_RUN60 = 15.0          # 60日涨幅≥15%
FADE_TOP_PULLBACK = 3.0    # 且已从20日高回落≥3% → 高位回落
FADE_SPIKE_DAY = 3.0       # 当日涨≥3% → 追涨日
FADE_WEAK_PULLBACK = 5.0   # 离20日高≥5% 或收在MA20下，且未止跌 → 弱势未止跌
FADE_NOTE = ('推后即跌形态一律不推（3A/Astra 同口径，没有就是0只）：①高位回落＝60日涨≥15%且已从20日高回落≥3%'
             '（例：TCL电子10/09、睿创微纳10/12）；②追涨日＝当日涨≥3%；③弱势未止跌＝离20日高≥5%或收在MA20下，'
             '且5日内创20日新低或5日未翻红。依据：V88 自身253条推荐复盘，命中者10日均−4.4%、69%下跌，'
             '其余+0.9%、44%下跌，前后两半时段各自成立。')


def fade_risk(q):
    """V88 推荐复盘得出的「推了就跌」三种形态；命中返回理由，否则 None。缺数据的那条不判（不凭空拦）。"""
    q = q or {}
    e = q.get('structure') or {}
    last = q.get('last')
    if not (_finite(last) and last > 0):
        return None
    ph20, r60 = e.get('previous_high20'), e.get('return60_pct')
    off_high = (last / ph20 - 1) * 100 if _finite(ph20) and ph20 > 0 else None
    if _finite(r60) and r60 >= FADE_RUN60 and off_high is not None and off_high <= -FADE_TOP_PULLBACK:
        return (f'高位回落：60日已涨{r60:.1f}%，现价已从20日高{ph20:g}回落{-off_high:.1f}%'
                f'（涨多了开始掉头，复盘10日均−7.7%）')
    day = q.get('change_pct')
    if _finite(day) and day >= FADE_SPIKE_DAY:
        return f'追涨日：当日已涨{day:.1f}%（复盘追涨日推荐10日均−4.9%），等回落再看'
    ma20, low5, low20, r5 = e.get('ma20'), e.get('low5'), e.get('low20'), e.get('return5_pct')
    weak = ((off_high is not None and off_high <= -FADE_WEAK_PULLBACK)
            or (_finite(ma20) and ma20 > 0 and last < ma20))
    new_low = _finite(low5) and _finite(low20) and low20 > 0 and low5 <= low20 * 1.005
    no_turn = _finite(r5) and r5 <= 0
    if weak and (new_low or no_turn):
        why = '5日内创20日新低' if new_low else f'5日未翻红（{r5:+.1f}%）'
        where = (f'离20日高{-off_high:.1f}%' if off_high is not None and off_high <= -FADE_WEAK_PULLBACK
                 else f'收在MA20 {ma20:g}下')
        return f'弱势未止跌：{where}且{why}（复盘10日均−5.1%）'
    return None


# 市场宽度＝收盘>MA60 的个股占比（与 horizon_book 回测同一定义）。各宽度区间启用哪些形态、给哪一档，只查 horizon_book.CELLS。
BREADTH_LOW = 0.35
BREADTH_HIGH = 0.50
BREADTH_MIN_SAMPLE = 30
BREADTH_LIVE_SAMPLE = 200
ASTRA_STOP_PCT = (5.0, 8.0)     # 用户约定：每笔止损距离 5-8%（1A 超短线按回测口径 2-8%）
ASTRA_MIN_RR = 1.5
# 证据表给出的买入：regime.band＝'horizon'。其回测样本本身就含弱势/追涨日，推后即跌形态不再另拦。
EVIDENCE_BANDS = ('horizon',)


def annual_down(q=None, row=None):
    """年度结构下行（现价<下倾MA200且MA60<MA120，trend_regime 同一定义）。"""
    a = ((q or {}).get('structure') or {}).get('annual') or {}
    return (a.get('regime') or (row or {}).get('annual_regime')) == 'down'


def fade_exempt(fade, grade, down, bottom=False):
    """唯一豁免口径：证据表（horizon_book）给 buy 时（bottom=True）全部放行——回测样本已含弱势/追涨日；
    其余情况（旧行复核）只有"弱势未止跌"在 buy 级且非年线下行时放行。"""
    # An historical winning cell cannot waive current high-position deterioration.
    return bool(fade and str(fade).startswith('弱势未止跌') and grade == 'buy' and not down)


# 底部确认（2026-11-10 用户：影石/小米22/腾讯371 底部不推、涨上来才推易被洗）：只作"底部雷达"观察描述，
# 买入与否、给几档统一查 horizon_book（其中"低位企稳"形态即底部启动的回测口径）。
BOTTOM_MIN_DD = 15.0
BOTTOM_AGE = (2, 8)
BOTTOM_BOUNCE = 1.03
BOTTOM_STOP_BUFFER = 0.99
RS_MIN_UNIVERSE = 30
_QUALITY = None


def quality_codes():
    global _QUALITY
    if _QUALITY is None:
        try:
            _QUALITY = {x[0] for x in json.loads((BASE / 'quality_pool.json').read_text(encoding='utf-8'))}
        except Exception:
            _QUALITY = set()
    return _QUALITY


def bottom_confirm(q):
    """底部确认形态（底部雷达的观察描述）；不满足或缺字段返回 None（不凭空判定）。"""
    q = q or {}
    e = q.get('structure') or {}
    last = q.get('last')
    hi60, low10, age, low60, ma5, r3, ma20 = (e.get(k) for k in
                                              ('high60', 'low10', 'low10_age', 'low60', 'ma5', 'return3_pct', 'ma20'))
    if not all(_finite(x) for x in (last, hi60, low10, age, low60, ma5, r3)) or min(last, hi60, low10, low60) <= 0:
        return None
    dd = (last / hi60 - 1) * 100
    if not (dd <= -BOTTOM_MIN_DD and low10 <= low60 * 1.001 and BOTTOM_AGE[0] <= age <= BOTTOM_AGE[1]
            and last >= low10 * BOTTOM_BOUNCE and r3 > 0 and last > ma5):
        return None
    stop = round(low10 * BOTTOM_STOP_BUFFER, 4)
    above = _finite(ma20) and last >= ma20
    return {'low': low10, 'age': int(age), 'drawdown_pct': round(dd, 1), 'bounce_pct': round((last / low10 - 1) * 100, 1),
            'stop': stop, 'stop_pct': round((1 - stop / last) * 100, 2), 'ma20': ma20, 'above_ma20': bool(above),
            'quality': q.get('code') in quality_codes(),
            # 止损>8%（V形急拉）时不追：回踩到这个价，止损距离正好8%
            'pullback_price': round(stop / 0.92, 4)}


def attach_rs(rows):
    """按市场写 structure['rs20_pct']＝20日涨幅在当轮候选池中的分位（0-1）；样本<30的市场不写。"""
    by = {}
    for q in rows or []:
        e = q.get('structure')
        if isinstance(e, dict) and _finite(e.get('return20_pct')):
            by.setdefault(q.get('market'), []).append(e)
    for group in by.values():
        if len(group) < RS_MIN_UNIVERSE:
            continue
        vals = sorted(e['return20_pct'] for e in group)
        n = len(vals)
        for e in group:
            e['rs20_pct'] = round(bisect.bisect_right(vals, e['return20_pct']) / n, 4)
    return rows


def playbook(market, breadth, tiers=None):
    """当前市场状态下启用的形态（与 entry_timing 同一张 horizon_book 证据表），供空席原因、日报、看板说明。"""
    from horizon_book import active, BAND_NAMES, TIERS, HK_NOTE, NO_RULE
    band = breadth_band(breadth)
    if band is None:
        return {'band': None, 'active': [], 'text': f'{market}市场宽度缺失，无法判定启用形态'}
    rows = active(market, breadth, tiers or TIERS + ('Astra',))
    head = f'{market}宽度{breadth:.0%}（{BAND_NAMES[band]}）'
    if not rows:
        return {'band': band, 'active': [], 'text': head + ('：' + HK_NOTE if market == '港股' else
                                                          '：' + NO_RULE + '（本宽度区间无前后两半段胜率均≥53%的规则），'
                                                          '只给学习型候选（轻仓跟踪复盘）')}
    by = {}
    for a in rows:
        by.setdefault(a['tier'], []).append(f'{a["setup"]}·{a["group"]}' + ('（证据弱）' if a['weak'] else ''))
    order = ('3A', '2A', '1A', 'Astra')
    text = '；'.join(f'{t}：' + '、'.join(by[t]) for t in order if t in by)
    return {'band': band, 'active': rows, 'text': head + '：启用 ' + text}


def entry_timing(q, breadth, bottom_rebound=False, tiers=None):
    """Astra 与 3A/2A/1A 共用的唯一进入时机判定：只查 horizon_book 周期证据表。
    tiers 默认 3A/2A/1A（多档命中取 3A>2A>1A）；Astra 传 ('Astra',)。命中且价格计划成立才 ok。"""
    from horizon_book import match, best, TIERS
    tiers = tuple(tiers or TIERS)
    hb = match(q, breadth, tiers)
    hit = best(hb, tiers)
    fade = fade_risk(q) or None
    bc = bottom_confirm(q)
    if hit:
        from horizon_book import SETUP_NAMES
        name = SETUP_NAMES[hit['setup']]
        pos = {'state': name, 'ok': True, 'signals': [name], 'reason': hit['reason'], 'horizon': hit}
        regime = {'grade': 'buy', 'band': 'horizon', 'reason': hit['reason'], 'breadth': breadth, 'tier': hit['tier'],
                  'cell': hit['cell'], 'weak': hit['weak']}
        from entry_progression import location
        loc = location(q)
        return {'position': pos, 'regime': regime, 'fade': fade, 'fade_block': loc['blocked'], 'ok': True, 'bottom': bc,
                'entry_location': loc,
                'horizon': hb, 'hit': hit, 'plan': hit['plan']}
    why = '；'.join(f'{t}{hb["misses"][t]}' for t in tiers if t in hb['misses']) or '未命中证据表'
    pct = f'宽度{breadth:.0%}' if _finite(breadth) else '宽度缺失'
    reason = f'{q.get("market")}{pct}：{why}'
    pos = {'state': '未命中证据表形态', 'ok': False, 'signals': [], 'reason': reason}
    return {'position': pos, 'regime': {'grade': 'watch', 'band': hb['band'], 'reason': reason, 'breadth': breadth,
                                        'tier': None, 'cell': None},
            'fade': fade, 'fade_block': False, 'ok': False, 'bottom': bc, 'horizon': hb, 'hit': None, 'plan': None}


def plan_geometry(row, last):
    """指令同口径：买价=max(现价, 计划上沿)，止损=指令止损（row['stop']，缺则止损区间下沿）。"""
    er = row.get('entry_range') or []
    ref = max(last, er[1]) if _finite(last) and len(er) == 2 and _finite(er[1]) else last
    stop = row.get('stop') if _finite(row.get('stop')) else (row.get('stop_range') or [None])[0]
    geo = stop_geometry(ref, stop, row.get('target'))
    return geo and geo | {'ref_price': round(ref, 4), 'stop_price': stop}


def stop_geometry(last, stop_px, target, fee=.5):
    """回测同一口径：现价入场、结构止损上沿、首段目标；返回止损距离%、净空间%、净盈亏比。"""
    if not (_finite(last) and _finite(stop_px) and _finite(target)) or last <= 0:
        return None
    stop_pct = (1 - stop_px / last) * 100
    space = (target / last - 1) * 100 - fee
    rr = space / (stop_pct + fee) if stop_pct > -fee else None
    return {'stop_pct': round(stop_pct, 2), 'net_space_pct': round(space, 2),
            'rr_backtest_basis': round(rr, 2) if rr is not None else None}


def market_breadth(rows=None, base=BASE):
    """各市场宽度=收盘（盘中为现价）高于 MA60 的个股占比，与回测同一定义。优先当轮全量行情；
    样本不足时用 screen_history 日线中覆盖≥半数个股的最近交易日。不触发抓取。"""
    out = {}
    by = {}
    for q in rows or []:
        e = q.get('structure') or {}
        if _finite(q.get('last')) and _finite(e.get('ma60')) and e['ma60'] > 0:
            by.setdefault(q.get('market'), []).append(q['last'] > e['ma60'])
    for market, hits in by.items():
        if len(hits) >= BREADTH_LIVE_SAMPLE:
            out[market] = {'value': round(sum(hits) / len(hits), 3), 'n': len(hits), 'asof': None,
                           'source': '当轮全量行情（现价>MA60占比）'}
    try:
        from screen_history import load
        series = {}
        for r in (load(base).get('records') or {}).values():
            bars = r.get('bars') or []
            if r.get('market') and r['market'] not in out and len(bars) >= 60:
                series.setdefault(r['market'], []).append(bars)
        for market, group in series.items():
            dates = {}
            for bars in group:
                for b in bars[-10:]:
                    dates[b[0]] = dates.get(b[0], 0) + 1
            day = max((d for d, c in dates.items() if c >= len(group) / 2), default=None)
            hits = []
            for bars in group:
                idx = next((i for i in range(len(bars) - 1, max(len(bars) - 11, -1), -1) if bars[i][0] == day), None)
                if idx is not None and idx >= 59:
                    hits.append(bars[idx][2] > sum(x[2] for x in bars[idx - 59:idx + 1]) / 60)
            if len(hits) >= BREADTH_MIN_SAMPLE:
                out[market] = {'value': round(sum(hits) / len(hits), 3), 'n': len(hits), 'asof': day,
                               'source': 'screen_history 全市场日线（收盘>MA60占比）'}
    except Exception:
        pass
    # 本机 screen_history 不随同步更新；云端当轮宽度已随 strategy_board/astra_cycle 同步下来，取较新的一份。
    for market, b in synced_breadth(base).items():
        if market in by and len(by[market]) >= BREADTH_LIVE_SAMPLE:
            continue
        mine = out.get(market)
        if not mine or (mine.get('asof') or '') < b['asof']:
            out[market] = b
    return out


def synced_breadth(base=BASE):
    import json
    from pathlib import Path
    found = {}
    for name, path in (('strategy_board.json', ('ladder', 'breadth')), ('astra_cycle.json', ('market_breadth',))):
        try:
            doc = json.loads((Path(base) / 'data' / name).read_text())
        except Exception:
            continue
        stamp = str(doc.get('generated_at') or '')[:10]
        node = doc
        for k in path:
            node = (node or {}).get(k) if isinstance(node, dict) else None
        for market, b in (node or {}).items():
            if not isinstance(b, dict) or not _finite(b.get('value')) or not stamp:
                continue
            asof = b.get('asof') or stamp
            if asof > (found.get(market) or {}).get('asof', ''):
                src = str(b.get('source') or '未注明')
                src = src if src.startswith('云端同步宽度') else f'云端同步宽度（{name}，{src}）'
                found[market] = {**b, 'asof': asof, 'source': src}
    return found


def breadth_band(value):
    if not _finite(value):
        return None
    return 'low' if value < BREADTH_LOW else 'mid' if value < BREADTH_HIGH else 'high'


def bottom_rebound_from_closes(pairs):
    """大盘触底反弹期：此前跌到 MA60 下方，现已离 20 日低反弹≥3% 且站上 MA20，仍未远离 MA60（≤MA60×1.02）。"""
    closes = [c for _, c in pairs]
    if len(closes) < 65:
        return False
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60
    low = min(closes[-20:])
    return bool(low < ma60 and closes[-1] >= low * REBOUND_FROM_LOW and closes[-1] > ma20 and closes[-1] <= ma60 * 1.02)


def env_from_closes(pairs):
    """回测口径的大盘环境：逆风/顺风/中性。pairs=[(date, close)] 升序。"""
    closes = [c for _, c in pairs]
    if len(closes) < 65:
        return 'unknown'
    ma20 = sum(closes[-20:]) / 20
    ma60 = sum(closes[-60:]) / 60
    ma20_prev = sum(closes[-25:-5]) / 20
    if closes[-1] < ma60 and ma20 < ma20_prev:
        return 'headwind'
    if closes[-1] > ma20 and closes[-1] > ma60 and ma20 > ma20_prev:
        return 'tailwind'
    return 'neutral'


def market_envs(base=BASE):
    """从受票据校验的指数收盘序列算出各市场环境（不触发任何抓取）。"""
    from benchmark_history import load, levels
    doc = load(Path(base) / 'data/benchmark_close_history.json')
    records = doc.get('records') or {}
    out = {}
    for market, code in MAIN_INDEX.items():
        rec = records.get(code)
        label, asof, rebound, ma20, ma60 = 'unknown', None, False, None, None
        if rec and rec.get('raw'):
            try:
                pairs = levels(code, rec['raw'])
                label = env_from_closes(pairs)
                rebound = bottom_rebound_from_closes(pairs)
                asof = pairs[-1][0]
                closes = [p[1] for p in pairs]
                if len(closes)>=20:ma20=sum(closes[-20:])/20
                if len(closes)>=60:ma60=sum(closes[-60:])/60
            except Exception:
                pass
        out[market] = {'index': code, 'env': label, 'asof': asof, 'bottom_rebound': rebound,
                       'ma20':ma20,'ma60':ma60,'source':'benchmark_close_history'}
    return out


def build(source, *, now=None, base=BASE, envs=None):
    """诊断文件：大盘环境、板块象限、深度回撤观察、反弹候选（与主榜同源）。不含任何平行 3A 裁决。"""
    now = now or datetime.now(timezone.utc)
    envs = envs or market_envs(base)
    rows = source.get('rows', [])
    quads = industry_quadrants(rows, base)
    quad_summary = {f'{mk}·{ind}': v for (mk, ind), v in quads.items() if v.get('quad') not in (None, '样本不足')}
    return {'version': VERSION, 'policy': POLICY, 'generated_at': now.isoformat(),
            'market_envs': envs, 'sector_quadrants': quad_summary,
            'deep_value_watch': deep_value_watch(rows, base),
            'rebound_buys': rebound_buys(rows, envs),
            'note': '诊断文件：入场质量判断已并入3A主榜（strategy_board.json 的 entry_gate / entry_blocked / rebound_buys），此处不另出名单。'}


def refresh(source, *, now=None, base=BASE):
    """挂在收盘刷新尾部：写独立文件，异常不影响主流程。"""
    from review_factpack import atomic_json
    doc = build(source, now=now, base=base)
    atomic_json(Path(base) / 'data/shadow_roster.json', doc)
    return doc
