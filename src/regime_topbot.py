"""V88·顶底研判层（Regime Top/Bottom Layer）

【2026-07-29 用户定纲】"在地顶的时候，对于大盘的判断很重要……先加入对大盘的分析"。

起因不是"系统没有大盘分析"，而是**已算出的原料没被组装成结论，且两套指标互相打架**：
  港股 trend=100 / breadth=100 / vol_heat=15 —— 教科书级的价涨量缩顶背离，
       旧 compute_turn_risk 却给"顶部转向风险 低(25/100)"；
  A股 放量杀跌(vol_heat=100、上证量比3.97)、20日 -7.5%，
       反而报 top_risk=30 —— 比过热的港股还高。

旧公式的三处判据错误（本模块逐条修正）：
  ① 35分权重挂在 indices[].turning 字段上，该字段常年为空 → 权重实际失效；
  ② "放量滞涨"要求 vol_ratio>=1.4，而**顶部的典型形态恰恰是缩量新高**（港股0.69），
     判据方向与现象相反，抓不到真顶；
  ③ chg5 < -2 直接加15分算顶部风险 —— 放量急跌是**底部特征**，被算进了顶部。

【诚实边界·铁律2】大盘顶部样本极少（A股 2007/2015/2021 就那么几次），
**本模块永不输出"顶部概率 X%"**——只输出"顶部特征命中 N/7"，逐条列出命中与未命中，
数据源缺失的条目显式标 ⬜ 而不是当作未命中。用户看到的是可数、可查、可推翻的清单。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

BJT = timezone(timedelta(hours=8))
BASE = Path(__file__).resolve().parent.parent
BREADTH_HIST = BASE / "data" / "breadth_history.json"

# 满值钝化判定：广度 >= 该值算"满"，连续天数 >= 该值算"钝化"
BREADTH_FULL = 90
BREADTH_STALL_DAYS = 3

# ── 阈值（2026-07-29 用当日三市场真实值校准，改动请重跑 selftest 复核）──
# 校准基准：港股(px74.2/vol37.9/背离+36.3/温度87/广度100) 应判"偏顶"≥3条；
#           A股(px5.5/vol100/背离-94.5/20日-14.3%) 应判"偏底"≥3条；
#           美股(px46.1/vol24.1/背离+22.0/温度50) 应判"中性"≤1条。
PX_HIGH = 70        # 价格分位≥此值＝高位（原80过严：恒指74.2也是明显高位）
PX_LOW = 20         # 价格分位≤此值＝低位
PX_MID = 60         # 背离判据的价格中位线
DIVERGE_TOP = 30    # 背离≥此值且价格在中位之上＝顶背离（价高量缩）
DIVERGE_BOT = -30   # 背离≤此值且价格在中位之下＝底背离（价低量增）


def _pct_rank(series, value) -> float | None:
    """value 在 series 中的百分位（0~100）。series 为可迭代数值。"""
    try:
        xs = [float(x) for x in series if x is not None]
        if len(xs) < 20:
            return None
        n = sum(1 for x in xs if x <= float(value))
        return round(100.0 * n / len(xs), 1)
    except Exception:
        logger.exception("[regime] _pct_rank 失败")
        return None


def price_volume_percentiles(close, volume, lookback: int = 60) -> dict:
    """【模块1·量价背离矩阵】计算价格分位与成交量分位。

    close/volume 为 pandas.Series（日线）。返回 {px_pct, vol_pct, diverge}。
    - px_pct  : 最新收盘在近 lookback 日收盘序列中的百分位
    - vol_pct : 近5日均量在近 lookback 日「5日滚动均量」序列中的百分位
                （用滚动均量而非单日量，避免单日异动干扰）
    - diverge : px_pct - vol_pct，正值大＝价高量缩（顶背离），负值大＝价低量增（底背离）
    """
    out = {"px_pct": None, "vol_pct": None, "diverge": None}
    try:
        if close is None or len(close) < 25:
            return out
        c = close.tail(lookback)
        out["px_pct"] = _pct_rank(c.tolist(), float(close.iloc[-1]))
        if volume is not None and len(volume) >= 25:
            v5 = volume.rolling(5).mean().dropna().tail(lookback)
            if len(v5) >= 20:
                out["vol_pct"] = _pct_rank(v5.tolist(), float(v5.iloc[-1]))
        if out["px_pct"] is not None and out["vol_pct"] is not None:
            out["diverge"] = round(out["px_pct"] - out["vol_pct"], 1)
    except Exception:
        logger.exception("[regime] price_volume_percentiles 失败")
    return out


def _load_breadth_hist() -> dict:
    try:
        if BREADTH_HIST.exists():
            return json.loads(BREADTH_HIST.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("[regime] 广度历史读取失败")
    return {}


def save_breadth_history(payload: dict) -> dict:
    """【模块2·广度时间序列】每次快照把各市场 breadth/temp/px_pct 存档。

    广度截面值无法区分两种含义：①刚从低位冲上100＝健康普涨
    ②在100横了很多天＝衰竭前兆（没有新股票能加入上涨了）。必须存历史才能分辨。
    返回 {market: {full_days, breadth_trend}}。
    """
    hist = _load_breadth_hist()
    today = datetime.now(BJT).strftime("%Y-%m-%d")
    stat = {}
    try:
        for market, mk in (payload or {}).items():
            t = (mk or {}).get("temperature") or {}
            if not t:
                continue
            row = hist.setdefault(market, {})
            row[today] = {"breadth": t.get("breadth"), "temp": t.get("temp"),
                          "vol_heat": t.get("vol_heat"), "trend": t.get("trend")}
            # 只留最近 120 天
            for d in sorted(row)[:-120]:
                row.pop(d, None)
            days = sorted(row)
            # 连续满值天数（含今日，往回数）
            full = 0
            for d in reversed(days):
                b = row[d].get("breadth")
                if b is not None and b >= BREADTH_FULL:
                    full += 1
                else:
                    break
            # 广度趋势：今日 vs 5日前
            trend_delta = None
            if len(days) >= 6:
                b_now, b_old = row[days[-1]].get("breadth"), row[days[-6]].get("breadth")
                if b_now is not None and b_old is not None:
                    trend_delta = round(float(b_now) - float(b_old), 1)
            stat[market] = {"full_days": full, "breadth_delta5": trend_delta,
                            "days_recorded": len(days)}
        BREADTH_HIST.parent.mkdir(parents=True, exist_ok=True)
        BREADTH_HIST.write_text(json.dumps(hist, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        logger.exception("[regime] 广度历史写入失败")
    return stat


def fetch_index_valuation(market: str) -> dict:
    """【模块5·估值水位】指数 PE 历史分位。

    顶底判断缺估值这一维是瘸的——技术面只能说"涨多了"，估值才能说"贵不贵"。
    当前状态：**数据源未接**，返回 {'pe_pct': None, 'source': None}。
    清单里显式显示 ⬜"数据源未接"，而不是当作"未命中"混进计数——
    假装有数据比没有数据更危险。

    【2026-07-29 实测记录，避免下次重复试错】
      ✗ 东财 push2 `stock/get` 指数 secid（1.000001／100.HSI／100.SPX）
        的 f162(PE-TTM)/f167(PB)/f173 全部返回 0 —— 指数标的本身无估值字段。
      ✗ 宽基 ETF 代理（510300/2800.HK/SPY）即使取到当前 PE，
        **也拿不到历史 PE 序列**，而分位必须要序列，只有点值没有意义。
      → 可行方向（需人工接）：中证指数官网月度估值 CSV／恒生指数公司月报／
        multpl.com 标普历史 PE。三者都需要爬页面或付费，不适合塞进日更流水线。
    在接上之前，本条在清单里永远显示 ⬜，且不计入"未命中"——
    宁可让用户看到"这一维还没有"，也不用技术指标伪装成估值。
    """
    return {"pe_pct": None, "source": None}


def topbot_checklist(market: str, indices: list, temp: dict,
                     breadth_stat: dict | None = None) -> dict:
    """【核心】顶/底特征清单，各7条。返回命中数与逐条明细。

    不给概率，只给"命中 N/7"+逐条理由。⬜ 表示数据源缺失，不计入分母的已知部分。
    """
    try:
        if not indices or not temp:
            return {}
        n = len(indices)
        avg = lambda k: sum(float(ix.get(k, 0) or 0) for ix in indices) / n
        vs20, chg5, chg20 = avg("vs_ma20"), avg("chg5d"), avg("chg20d")
        breadth = float(temp.get("breadth", 50) or 50)
        t = float(temp.get("temp", 50) or 50)
        # 分位：取各指数均值（部分指数可能没算出来）
        pxs = [ix.get("px_pct") for ix in indices if ix.get("px_pct") is not None]
        vols = [ix.get("vol_pct") for ix in indices if ix.get("vol_pct") is not None]
        px_pct = round(sum(pxs) / len(pxs), 1) if pxs else None
        vol_pct = round(sum(vols) / len(vols), 1) if vols else None
        diverge = round(px_pct - vol_pct, 1) if (px_pct is not None and vol_pct is not None) else None
        full_days = (breadth_stat or {}).get("full_days") or 0
        b_delta5 = (breadth_stat or {}).get("breadth_delta5")
        val = fetch_index_valuation(market)

        def item(key, ok, text):
            """ok: True命中 / False未命中 / None数据源缺失"""
            return {"key": key, "hit": ok, "text": text}

        # ── 顶部 7 条 ──
        top = [
            item("价格高位",
                 (px_pct >= PX_HIGH) if px_pct is not None else (vs20 >= 4.0),
                 f"价格分位{px_pct}" if px_pct is not None else f"距20日均线{vs20:+.2f}%"),
            # 【2026-07-29 首跑真实数据后校准】原判据要求 px_pct>=75 且 vol_pct<=40 双门槛,
            # 港股实测 px=74.2/vol=37.9(背离+36.3,典型缩量上涨)因差0.8未命中,
            # 导致 checklist(1/7) 与 turn_risk(65高) 互相打架——正是本模块要消灭的毛病。
            # 改为以**背离幅度**为主判据:幅度够大且价格在中位之上即算顶背离。
            item("量价背离",
                 (diverge >= DIVERGE_TOP and px_pct >= PX_MID) if diverge is not None else None,
                 f"价格分位{px_pct}／量分位{vol_pct}＝背离{diverge:+.1f}" if diverge is not None
                 else "⬜价量分位未取到"),
            item("广度钝化",
                 (breadth >= BREADTH_FULL and full_days >= BREADTH_STALL_DAYS),
                 f"广度{breadth:.0f}·满值连续{full_days}日"
                 + (f"·5日变动{b_delta5:+.1f}" if b_delta5 is not None else "（历史仅{}天，需攒够{}天）".format(
                     (breadth_stat or {}).get("days_recorded", 0), BREADTH_STALL_DAYS))),
            item("动量衰竭", (chg20 > 4 and chg5 < chg20 / 4),
                 f"20日{chg20:+.2f}%但5日仅{chg5:+.2f}%"),
            item("估值高位", None if val["pe_pct"] is None else val["pe_pct"] >= 80,
                 "⬜估值数据源未接" if val["pe_pct"] is None else f"PE分位{val['pe_pct']}"),
            item("温度过热", t >= 75, f"温度{t:.0f}"),
            item("指数拐点", any(str(ix.get("turning", "")).startswith("⚠️") for ix in indices),
                 "引擎拐点信号" if any(ix.get("turning") for ix in indices) else "无拐点信号"),
        ]
        # ── 底部 7 条 ──
        bot = [
            item("价格低位",
                 (px_pct <= PX_LOW) if px_pct is not None else (vs20 <= -4.0),
                 f"价格分位{px_pct}" if px_pct is not None else f"距20日均线{vs20:+.2f}%"),
            item("放量杀跌",
                 (diverge <= DIVERGE_BOT and px_pct <= (100 - PX_MID)) if diverge is not None else None,
                 f"价格分位{px_pct}／量分位{vol_pct}＝背离{diverge:+.1f}" if diverge is not None
                 else "⬜价量分位未取到"),
            item("广度极低", breadth <= 20, f"广度{breadth:.0f}"),
            item("深度超跌", chg20 <= -10, f"20日{chg20:+.2f}%"),
            item("估值低位", None if val["pe_pct"] is None else val["pe_pct"] <= 20,
                 "⬜估值数据源未接" if val["pe_pct"] is None else f"PE分位{val['pe_pct']}"),
            item("温度冰点", t <= 30, f"温度{t:.0f}"),
            item("指数底拐", any(str(ix.get("turning", "")).startswith("🔄") for ix in indices),
                 "引擎底拐信号" if any(ix.get("turning") for ix in indices) else "无拐点信号"),
        ]
        cnt = lambda L: (sum(1 for x in L if x["hit"] is True),
                         sum(1 for x in L if x["hit"] is None))
        top_hit, top_na = cnt(top)
        bot_hit, bot_na = cnt(bot)
        return {"market": market, "top": top, "bottom": bot,
                "top_hit": top_hit, "top_na": top_na,
                "bottom_hit": bot_hit, "bottom_na": bot_na,
                "px_pct": px_pct, "vol_pct": vol_pct, "diverge": diverge,
                "note": "样本<5不给概率(铁律2)——只报命中条数,⬜=数据源缺失非未命中"}
    except Exception:
        logger.exception(f"[regime] topbot_checklist 失败 market={market}")
        return {}


def compute_turn_risk_v2(indices, temp, checklist: dict | None = None) -> dict | None:
    """【模块3·修正版转向风险】替换旧 compute_turn_risk。

    与旧版的三处差异（均由 2026-07-29 实测反例逼出）：
      ① 不再把 35 分押在常年为空的 turning 字段上，降为 10 分的加成项；
      ② **缩量新高**（量价背离）成为顶部主判据（旧版要求放量，方向相反）；
      ③ 放量急跌归入 bottom_opp，不再计入 top_risk。
    分数仍是规则合成、可复算，但结论以 checklist 命中数为准——分数只用于排序。
    """
    try:
        if not indices or not temp:
            return None
        n = len(indices)
        t = float((temp or {}).get("temp", 50) or 50)
        avg = lambda k: sum(float(ix.get(k, 0) or 0) for ix in indices) / n
        chg5, chg20, vs20 = avg("chg5d"), avg("chg20d"), avg("vs_ma20")
        vr = sum(float(ix.get("vol_ratio", 1) or 1) for ix in indices) / n
        cl = checklist or {}
        diverge = cl.get("diverge")
        px_pct, vol_pct = cl.get("px_pct"), cl.get("vol_pct")

        risk = 0.0
        # 判据与 topbot_checklist 共用同一组阈值常量——两者若各用一套，
        # 就会重演"清单说1/7、风险分说65高"的打架（2026-07-29 首跑实测）。
        if diverge is not None and diverge >= DIVERGE_TOP and px_pct >= PX_MID:
            risk += 35                                  # ★ 缩量新高＝顶背离（新主判据）
        elif vs20 >= 4.0 and vr < 0.9:
            risk += 25                                  # 分位取不到时的降级判据
        risk += 25 if t >= 75 else (12 if t >= 60 else 0)
        if float(temp.get("breadth", 50) or 50) >= BREADTH_FULL:
            risk += 15                                  # 广度打满＝没有新的上涨来源
        if chg20 > 4 and chg5 < chg20 / 4:
            risk += 15                                  # 动量衰竭
        if any(str(ix.get("turning", "")).startswith("⚠️") for ix in indices):
            risk += 10

        opp = 0.0
        if diverge is not None and diverge <= DIVERGE_BOT and px_pct <= (100 - PX_MID):
            opp += 35                                   # ★ 放量杀跌＝底背离（原被误算进顶部）
        elif vs20 <= -4.0 and vr >= 1.4:
            opp += 25
        opp += 25 if t <= 30 else (12 if t <= 40 else 0)
        if chg20 <= -10:
            opp += 15
        if float(temp.get("breadth", 50) or 50) <= 20:
            opp += 15
        if any(str(ix.get("turning", "")).startswith("🔄") for ix in indices):
            opp += 10

        risk, opp = int(min(100, risk)), int(min(100, opp))
        _lv = lambda x: "高" if x >= 55 else ("中" if x >= 30 else "低")
        txt = f"顶部转向风险 {_lv(risk)}({risk}/100) ｜ 底部转机信号 {_lv(opp)}({opp}/100)"
        if cl:
            txt += f"｜顶部特征{cl.get('top_hit', 0)}/7·底部特征{cl.get('bottom_hit', 0)}/7"
        return {"top_risk": risk, "bottom_opp": opp, "text": txt, "ver": "v2"}
    except Exception:
        logger.exception("[regime] compute_turn_risk_v2 失败")
        return None


def phase_spread(payload: dict) -> dict:
    """【模块4·三市场相位差】三个市场温度的分化程度本身就是信号。

    2026-07-29 实测：港股82🔥 / 美股50🟡 / A股33🟠，极差49点。
    只给事实与分级，**解读留给 Claude 每班一行**（判断不写死进规则）。
    """
    try:
        temps = {m: (mk.get("temperature") or {}).get("temp")
                 for m, mk in (payload or {}).items()}
        temps = {m: v for m, v in temps.items() if v is not None}
        if len(temps) < 2:
            return {}
        hi = max(temps, key=temps.get)
        lo = min(temps, key=temps.get)
        spread = temps[hi] - temps[lo]
        level = "极端分化" if spread >= 40 else ("明显分化" if spread >= 25 else "同步")
        return {"temps": temps, "hot": hi, "cold": lo, "spread": spread, "level": level,
                "text": f"{level}：{hi}{temps[hi]} vs {lo}{temps[lo]}，极差{spread}点"}
    except Exception:
        logger.exception("[regime] phase_spread 失败")
        return {}


def build(payload: dict) -> dict:
    """总装：给 market_snapshot 的 payload 补上顶底研判层。原地补字段并返回汇总。"""
    out = {"generated_at": datetime.now(BJT).strftime("%Y-%m-%d %H:%M（北京时间）"),
           "note": "V88·顶底研判层：命中条数制,不给顶底概率(样本<5·铁律2)",
           "markets": {}}
    try:
        breadth_stat = save_breadth_history(payload)
        for market, mk in (payload or {}).items():
            temp = (mk or {}).get("temperature") or {}
            idx = (mk or {}).get("indices") or []
            cl = topbot_checklist(market, idx, temp, breadth_stat.get(market))
            tr = compute_turn_risk_v2(idx, temp, cl)
            if cl:
                mk["topbot"] = cl
            if tr:
                mk["turn_risk"] = tr          # 覆盖旧版（旧版判据方向错误，见模块docstring）
            out["markets"][market] = {"topbot": cl, "turn_risk": tr,
                                      "breadth_stat": breadth_stat.get(market)}
        out["phase_spread"] = phase_spread(payload)
    except Exception:
        logger.exception("[regime] build 失败")
    return out


def selftest() -> bool:
    """阈值回归测试：用 2026-07-29 三市场真实值做基准，改阈值后必须重跑。

    这三组值是本模块的**校准锚**——它们不是编的，是当天实盘抓出来的：
      港股 缩量上冲(背离+36.3、温度87、广度100) → 该判"偏顶"
      A股  放量杀跌(背离-94.5、20日-14.3%)      → 该判"偏底"
      美股 中性(背离+22.0、温度50)               → 该判"中性"
    `python3 src/regime_topbot.py` 直接跑。
    """
    cases = [
        ("港股", [{"px_pct": 74.2, "vol_pct": 37.9, "vs_ma20": 5.64, "chg5d": 3.89,
                   "chg20d": 10.70, "vol_ratio": 0.77, "turning": ""}],
         {"temp": 87, "breadth": 100}, "top", 3),
        ("A股", [{"px_pct": 5.5, "vol_pct": 100.0, "vs_ma20": -7.29, "chg5d": -3.1,
                  "chg20d": -14.29, "vol_ratio": 2.65, "turning": ""}],
         {"temp": 39, "breadth": 60}, "bottom", 3),
        ("美股", [{"px_pct": 46.1, "vol_pct": 24.1, "vs_ma20": -1.20, "chg5d": -1.26,
                   "chg20d": -0.91, "vol_ratio": 0.95, "turning": ""}],
         {"temp": 50, "breadth": 73}, "neutral", 1),
    ]
    ok = True
    for market, idx, temp, expect, bound in cases:
        cl = topbot_checklist(market, idx, temp, {"full_days": 0, "days_recorded": 1})
        tr = compute_turn_risk_v2(idx, temp, cl)
        th, bh = cl.get("top_hit", 0), cl.get("bottom_hit", 0)
        risk, opp = tr.get("top_risk", 0), tr.get("bottom_opp", 0)
        if expect == "top":
            good = th >= bound and risk > opp
        elif expect == "bottom":
            good = bh >= bound and opp > risk
        else:
            good = th <= bound and bh <= bound
        # 一致性：清单与风险分不许指向相反方向（本模块存在的理由）
        consist = not ((th > bh and opp > risk) or (bh > th and risk > opp))
        flag = "✅" if (good and consist) else "❌"
        if not (good and consist):
            ok = False
        print(f"{flag} {market}: 顶{th}/7 底{bh}/7 | 顶险{risk} 底机{opp} | 期望={expect}"
              + ("" if consist else "  ⚠️清单与风险分方向相反"))
    print("selftest:", "PASS" if ok else "FAIL")
    return ok


def render_lines(regime: dict) -> list[str]:
    """渲染成三端可直接用的文本行。"""
    lines = []
    try:
        ps = regime.get("phase_spread") or {}
        if ps:
            lines.append(f"🌐 三市场相位：{ps.get('text')}")
        for market, r in (regime.get("markets") or {}).items():
            cl = r.get("topbot") or {}
            if not cl:
                continue
            side = "顶" if cl.get("top_hit", 0) >= cl.get("bottom_hit", 0) else "底"
            hit = cl.get("top_hit", 0) if side == "顶" else cl.get("bottom_hit", 0)
            na = cl.get("top_na", 0) if side == "顶" else cl.get("bottom_na", 0)
            lines.append(f"🌡️ {market}·{side}部特征 {hit}/7"
                         + (f"（其中{na}条数据源未接）" if na else "")
                         + f"　{(r.get('turn_risk') or {}).get('text', '')}")
            for it in (cl.get("top") if side == "顶" else cl.get("bottom")) or []:
                mark = "✅" if it["hit"] is True else ("⬜" if it["hit"] is None else "▫️")
                lines.append(f"　　{mark} {it['key']}：{it['text']}")
    except Exception:
        logger.exception("[regime] render_lines 失败")
    return lines


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(0 if selftest() else 1)


def transmission_check() -> dict:
    """【三系统一致性验证】大盘→个股的传导有没有真的落到三套判断上。

    2026-07-29 用户定纲"把这个逻辑三系统推广验证执行"。本函数是那句"验证"的落地：
    每班跑一次，任何一方没吃到大盘顶底事实就报出来——**沉默的脱节比错误更危险**
    （港股曾同时被 env_gate 判 bull·标准仓、被 CS7 判过热驳回，两套结论互不知情）。
    """
    import subprocess
    out = {"checked_at": datetime.now(BJT).strftime("%Y-%m-%d %H:%M"), "rows": [], "ok": True}
    try:
        reg = json.loads((BASE / "data" / "regime_topbot.json").read_text(encoding="utf-8"))
        for mk, r in (reg.get("markets") or {}).items():
            cl, tr = (r.get("topbot") or {}), (r.get("turn_risk") or {})
            th, bh = cl.get("top_hit", 0), cl.get("bottom_hit", 0)
            risk, opp = tr.get("top_risk", 0), tr.get("bottom_opp", 0)
            row = {"market": mk, "top_hit": th, "bottom_hit": bh,
                   "top_risk": risk, "bottom_opp": opp}
            # ① 规则引擎：market_regime 是否吃到顶底层
            try:
                import sys as _s
                _s.path.insert(0, str(BASE / "src"))
                from v88_decision_core import market_regime
                rg = market_regime(mk)
                row["engine_regime"] = rg.get("regime")
                row["engine_saw_topbot"] = (rg.get("top_hit") is not None)
                row["engine_topish"] = bool(rg.get("topish"))
                row["engine_botish"] = bool(rg.get("botish"))
                # 一致性：顶部特征≥3 却仍给 bull＝传导断了
                if th >= 3 and rg.get("regime") == "bull":
                    row["ERROR"] = "顶部特征≥3 但引擎仍判 bull——传导断裂"
                    out["ok"] = False
            except Exception:
                logger.exception("[transmission] 规则引擎侧检查失败")
                row["ERROR"] = "engine check failed"
                out["ok"] = False
            # ② GPT：盲审 prompt 是否带上大盘顶底事实
            try:
                from gpt_review import build_prompt
                p = build_prompt([{"name": "X", "code": "TEST", "last": 1, "pos52": 50}])
                row["gpt_saw_topbot"] = ("大盘顶底特征" in p)
                if not row["gpt_saw_topbot"]:
                    row["ERROR"] = "GPT prompt 未含大盘顶底事实"
                    out["ok"] = False
            except Exception:
                logger.exception("[transmission] GPT侧检查失败")
                row["gpt_saw_topbot"] = None
            # ③ Claude：本班复核是否提到该市场的顶底结论（fable_review.md 当日段）
            try:
                txt = (BASE / "data" / "fable_review.md").read_text(encoding="utf-8")[:6000]
                today = datetime.now(BJT).strftime("%Y-%m-%d")
                seg = txt.split(today)[1][:2500] if today in txt else ""
                row["claude_mentioned"] = bool(seg) and (
                    "顶部特征" in seg or "底部特征" in seg or "顶底" in seg)
            except Exception:
                logger.exception("[transmission] Claude侧检查失败")
                row["claude_mentioned"] = None
            out["rows"].append(row)
        (BASE / "data" / "transmission_check.json").write_text(
            json.dumps(out, ensure_ascii=False, indent=1), encoding="utf-8")
    except Exception:
        logger.exception("[transmission] 验证失败")
        out["ok"] = False
    return out
