"""
cloud_engine.py — 云端版个股搜索引擎（自包含·免重引擎·Streamlit Cloud 可跑）
只用 yfinance + pandas，精简复用桌面版「趋势脉搏」核心逻辑：
趋势分0-100 / 趋势阶段7档 / 量价关系 / 明确动作 / 支撑压力 / 失效条件 / 原因。
不依赖 app_v88_integrated（14k行引擎在免费云跑不动），逻辑口径与桌面版一致。
"""
import pandas as pd


# 常用中文名/拼音/别名 → yahoo 代码（简搜：手机直接打名字）
NAME_MAP = {
    # 港股
    "腾讯": "0700.HK", "腾讯控股": "0700.HK", "tencent": "0700.HK",
    "阿里": "9988.HK", "阿里巴巴": "9988.HK", "alibaba": "9988.HK",
    "美团": "3690.HK", "小米": "1810.HK", "京东": "9618.HK", "快手": "1024.HK",
    "比亚迪港": "1211.HK", "中芯港": "0981.HK", "理想": "2015.HK", "小鹏": "9868.HK",
    "网易港": "9999.HK", "药明生物": "2269.HK", "友邦": "1299.HK", "港交所": "0388.HK",
    "中国移动港": "0941.HK", "中海油": "0883.HK", "中国平安港": "2318.HK", "李宁": "2331.HK",
    # A股
    "茅台": "600519.SS", "贵州茅台": "600519.SS", "宁德": "300750.SZ", "宁德时代": "300750.SZ",
    "五粮液": "000858.SZ", "比亚迪": "002594.SZ", "平安": "601318.SS", "中国平安": "601318.SS",
    "招行": "600036.SS", "招商银行": "600036.SS", "中芯国际": "688981.SS", "紫金": "601899.SS",
    "紫金矿业": "601899.SS", "隆基": "601012.SS", "美的": "000333.SZ", "海康": "002415.SZ",
    "恒瑞": "600276.SS", "长江电力": "600900.SS", "药明康德": "603259.SS", "东方财富": "300059.SZ",
    "海光": "688041.SS", "海光信息": "688041.SS", "中信证券": "600030.SS", "立讯": "002475.SZ",
    "立讯精密": "002475.SZ", "中国移动": "600941.SS", "工商银行": "601398.SS", "中国石油": "601857.SS",
    "中国神华": "601088.SS", "亨通光电": "600487.SS", "亨通": "600487.SS", "中际旭创": "300308.SZ",
    "北方华创": "002371.SZ", "寒武纪": "688256.SS", "韦尔股份": "603501.SS", "兆易创新": "603986.SS",
    "闻泰科技": "600745.SS", "汇川技术": "300124.SZ", "阳光电源": "300274.SZ", "通威股份": "600438.SS",
    "赛力斯": "601127.SS", "长城汽车": "601633.SS", "长安汽车": "000625.SZ", "京东方": "000725.SZ",
    "TCL科技": "000100.SZ", "紫光国微": "002049.SZ", "澜起科技": "688008.SS", "中微公司": "688012.SS",
    "药明康德A": "603259.SS", "爱尔眼科": "300015.SZ", "片仔癀": "600436.SS", "云南白药": "000538.SZ",
    "山西汾酒": "600809.SS", "泸州老窖": "000568.SZ", "洋河": "002304.SZ", "海天味业": "603288.SS",
    "牧原": "002714.SZ", "温氏": "300498.SZ", "三一重工": "600031.SS", "中国建筑": "601668.SS",
    "中国中免": "601888.SS", "顺丰": "002352.SZ", "格力": "000651.SZ", "海尔": "600690.SS",
    "科大讯飞": "002230.SZ", "金山办公": "688111.SS", "用友": "600588.SS", "恒生电子": "600570.SS",
    # 美股
    "苹果": "AAPL", "英伟达": "NVDA", "微软": "MSFT", "谷歌": "GOOG", "亚马逊": "AMZN",
    "特斯拉": "TSLA", "台积电": "TSM", "meta": "META", "脸书": "META", "奈飞": "NFLX",
    "美光": "MU", "博通": "AVGO", "超微": "AMD", "英特尔": "INTC", "甲骨文": "ORCL",
    "强生": "JNJ", "礼来": "LLY", "默沙东": "MRK", "辉瑞": "PFE", "可口可乐": "KO",
    "摩根大通": "JPM", "伯克希尔": "BRK-B", "spacex": "SPCX", "网易": "NTES", "拼多多": "PDD",
}


# ── 离线全市场名录（A股全部5200+/港股/美股，云端免网络秒搜）──
_NAMES_CACHE = None

def _load_names():
    global _NAMES_CACHE
    if _NAMES_CACHE is None:
        import json as _j
        from pathlib import Path as _P
        try:
            _NAMES_CACHE = _j.loads((_P(__file__).parent / "stock_names.json").read_text(encoding="utf-8"))
        except Exception:
            _NAMES_CACHE = []
    return _NAMES_CACHE


def search_candidates(query: str, limit: int = 15):
    """离线名录搜索：精确 > 前缀 > 包含。返回 [(name, code, market)]，重名全列出供选择。"""
    q = str(query).strip()
    if not q:
        return []
    names = _load_names()
    exact, prefix, contain = [], [], []
    ql = q.lower()
    for e in names:
        n = e["n"]
        if n == q:
            exact.append(e)
        elif n.startswith(q) or n.lower().startswith(ql):
            prefix.append(e)
        elif q in n or ql in n.lower():
            contain.append(e)
    out, seen = [], set()
    for e in exact + prefix + contain:
        if e["c"] not in seen:
            seen.add(e["c"])
            out.append((e["n"], e["c"], e["m"]))
        if len(out) >= limit:
            break
    # 名录未命中 → NAME_MAP 别名兜底（"亨通"/"宁德"等简称、拼音小写）
    if not out:
        for alias, code in NAME_MAP.items():
            if q == alias or ql == alias.lower() or (len(q) >= 2 and q in alias):
                mkt = ("港股" if code.endswith(".HK") else
                       ("A股" if code.endswith((".SS", ".SZ")) else "美股"))
                if code not in seen:
                    seen.add(code)
                    out.append((alias, code, mkt))
                if len(out) >= limit:
                    break
    return out


def name_of(code: str) -> str:
    """代码反查名称（用于结果标题显示全名）"""
    c = str(code).strip().upper()
    for e in _load_names():
        if e["c"].upper() == c:
            return e["n"]
    return ""


def _eastmoney_search(query: str):
    """东财搜索接口：任意中文名/拼音/代码 → yahoo 代码。与 V88 桌面同一数据源。
    MktNum: 1→.SS 沪  0→.SZ 深  116→.HK 港  155→美股。取第一个匹配。失败返回 None。"""
    import requests
    try:
        s = requests.Session(); s.trust_env = False
        r = s.get("https://searchapi.eastmoney.com/api/suggest/get",
                  params={"input": query, "type": "14",
                          "token": "D43BF722C8E33BDC906FB84D85E326E8", "count": 10},
                  proxies={"http": None, "https": None},  # 强制直连，不受代理环境干扰
                  headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
        data = (r.json().get("QuotationCodeTable") or {}).get("Data") or []
        for it in data:
            code = str(it.get("Code", "")).strip()
            mkt = str(it.get("MktNum", ""))
            if not code:
                continue
            if mkt == "1":
                return code + ".SS"
            if mkt == "0":
                return code + ".SZ"
            if mkt == "116":
                return code.zfill(4) + ".HK"
            if mkt == "155":
                return code  # 美股
        return None
    except Exception:
        return None


def to_yf(code: str) -> str:
    """归一化：中文名/拼音→代码；AAPL→AAPL｜0700→0700.HK｜600519→600519.SS｜000001→000001.SZ"""
    raw = str(code).strip()
    if raw in NAME_MAP:
        return NAME_MAP[raw]
    low = raw.lower()
    if low in NAME_MAP:
        return NAME_MAP[low]
    c = raw.upper()
    if "." in c:
        return c
    if c.isalpha():
        return c  # 美股代码
    if c.isdigit():
        if len(c) == 6:
            return c + (".SS" if c[0] in ("6", "5", "9") else ".SZ")
        if len(c) <= 5:
            return c.zfill(4) + ".HK"  # 港股
    # 含中文/拼音等 → 离线全市场名录(云端免网络) → 东财搜索兜底
    cands = search_candidates(raw, limit=1)
    if cands:
        return cands[0][1]
    hit = _eastmoney_search(raw)
    return hit if hit else c


def is_chinese_name(code: str) -> bool:
    """无法识别的中文名判断（东财也搜不到时才提示）"""
    raw = str(code).strip()
    if not any("一" <= ch <= "鿿" for ch in raw):
        return False
    return raw not in NAME_MAP and not search_candidates(raw, limit=1) and not _eastmoney_search(raw)


def fetch(symbol: str):
    import yfinance as yf
    try:
        df = yf.Ticker(symbol).history(period="6mo")
        return df if df is not None and len(df) >= 30 else None
    except Exception:
        return None

def _clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, x))


def analyze_trend_full(df, sector_strength=None):
    """
    【V99】综合量价趋势判断：8项分数拆解 + 9态量价 + 9段趋势 + 6级水位 +
    MACD/均线全细节 + 8种动作(带买入区/回踩点/加仓点/止损/减仓/失效)。
    sector_strength: 可选 0-100 板块热度(有则计入板块强度分，无则中性并标注)。
    返回结构化 dict，纯确定性计算。
    """
    try:
        if df is None or len(df) < 35:
            return None
        c = df["Close"].dropna()
        v = df["Volume"].fillna(0)
        hi, lo = df["High"], df["Low"]
        last = float(c.iloc[-1]); prev = float(c.iloc[-2])
        ma = {n: float(c.rolling(min(n, len(c))).mean().iloc[-1]) for n in (5, 10, 20, 55, 120)}
        ma20_series = c.rolling(20).mean()
        ma20_up = float(ma20_series.iloc[-1]) > float(ma20_series.iloc[-5]) if len(c) >= 25 else True

        # MACD
        dif = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        dea = dif.ewm(span=9, adjust=False).mean()
        hist = dif - dea
        macd_gold = float(dif.iloc[-1]) > float(dea.iloc[-1])
        just_cross = macd_gold and float(dif.iloc[-2]) <= float(dea.iloc[-2])
        just_dead = (not macd_gold) and float(dif.iloc[-2]) >= float(dea.iloc[-2])
        hist_now, hist_prev = float(hist.iloc[-1]), float(hist.iloc[-3])
        hist_expand = abs(hist_now) > abs(hist_prev)
        red = hist_now > 0
        # 背离(近30日)：价新高但DIF未新高=顶背离；价新低但DIF未新低=底背离
        div = ""
        if len(c) >= 30:
            p_hi_now = float(hi.tail(5).max()); p_hi_prev = float(hi.iloc[-25:-5].max())
            d_hi_now = float(dif.tail(5).max()); d_hi_prev = float(dif.iloc[-25:-5].max())
            p_lo_now = float(lo.tail(5).min()); p_lo_prev = float(lo.iloc[-25:-5].min())
            d_lo_now = float(dif.tail(5).min()); d_lo_prev = float(dif.iloc[-25:-5].min())
            if p_hi_now > p_hi_prev and d_hi_now < d_hi_prev:
                div = "⚠️顶背离(价新高MACD走弱)"
            elif p_lo_now < p_lo_prev and d_lo_now > d_lo_prev:
                div = "🌱底背离(价新低MACD走强)"

        macd_txt = (("金叉" if macd_gold else "死叉")
                    + ("·刚金叉" if just_cross else ("·刚死叉" if just_dead else ""))
                    + ("·红柱扩大" if (red and hist_expand) else ("·红柱缩小" if (red and not hist_expand) else
                       ("·绿柱扩大" if (not red and hist_expand) else "·绿柱缩小")))
                    + ("·" + div if div else ""))

        # RSI
        delta = c.diff()
        rs_ = delta.clip(lower=0).ewm(com=13).mean() / (-delta.clip(upper=0)).ewm(com=13).mean()
        rsi = float((100 - 100 / (1 + rs_)).iloc[-1])

        # 量能
        v5, v10, v20 = float(v.tail(5).mean()), float(v.tail(10).mean()), float(v.tail(20).mean()) or 1.0
        volr = v5 / v20
        vold = float(v.iloc[-1]) / v20
        vol_prev = float(v.iloc[-2]) or 1.0
        vol_up_today = float(v.iloc[-1]) > vol_prev
        chg1 = (last / prev - 1) * 100
        chg5 = (last / float(c.iloc[-6]) - 1) * 100 if len(c) >= 6 else 0.0
        chg20 = (last / float(c.iloc[-21]) - 1) * 100 if len(c) >= 21 else 0.0
        bias20 = (last / ma[20] - 1) * 100 if ma[20] else 0.0

        # 位置/关键位
        h60 = float(hi.tail(60).max()); l20 = float(lo.tail(20).min())
        l250 = float(lo.tail(min(250, len(lo))).min()); h250 = float(hi.tail(min(250, len(hi))).max())
        pos52 = (last - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
        new_high_60 = float(hi.iloc[-1]) >= h60 * 0.995
        near_ma20 = abs(last - ma[20]) / ma[20] < 0.02 if ma[20] else False
        support = round(max(ma[20], l20) if last > ma[20] else max(ma[55], l20), 2)
        resistance = round(h60 if last < h60 * 0.99 else h250, 2)

        # ── 均线细节 ──
        above = {n: last > ma[n] for n in (5, 10, 20, 55)}
        bull_align = last > ma[5] > ma[20] > ma[55]
        ma_txt = "、".join(f"MA{n}{'✅' if above[n] else '❌'}" for n in (5, 10, 20, 55))
        ma_state = ("多头排列" if bull_align else
                    ("站上MA20未站MA55" if above[20] and not above[55] else
                     ("跌破MA20" if not above[20] else "均线纠缠")))
        if near_ma20 and above[20] and ma20_up:
            ma_state += "·回踩MA20不破"

        # ── 9态量价关系 ──
        if chg5 > 2 and new_high_60 and volr >= 1.15:
            vp, vp_lv = "放量突破·强势确认", 2
        elif chg1 > 1 and vold >= 1.2:
            vp, vp_lv = "放量上涨·健康进攻", 2
        elif chg1 > 1 and vold < 0.85:
            vp, vp_lv = "缩量上涨·动力不足", 1
        elif chg1 > 0.3 and not vol_up_today:
            vp, vp_lv = "价涨量跌·反弹质量一般", 1
        elif chg1 < -1 and vold < 0.85:
            vp, vp_lv = "缩量回调·健康回踩", 1
        elif chg1 < -0.3 and not vol_up_today:
            vp, vp_lv = "价跌量缩·暂时正常调整", 1
        elif chg1 < -1 and vold >= 1.2:
            vp, vp_lv = "放量下跌·风险释放/资金出逃", 0
        elif chg1 < -0.3 and vol_up_today:
            vp, vp_lv = "价跌量增·趋势转弱风险", 0
        elif vold >= 1.5 and abs(chg1) < 1 and pos52 > 65:
            vp, vp_lv = "放量滞涨·高位分歧警惕出货", 0
        else:
            vp, vp_lv = "量价中性", 1

        # ── 6级水位 ──
        if pos52 < 25:
            water, water_adv, water_risk = "低位", "可试仓", 15
        elif pos52 < 40:
            water, water_adv, water_risk = "中低位", "可回踩买", 30
        elif pos52 < 60:
            water, water_adv, water_risk = "中位", "看趋势确认", 50
        elif pos52 < 78:
            water, water_adv, water_risk = "中高位", "不追高·只等回踩", 68
        elif pos52 < 92:
            water, water_adv, water_risk = "高位", "只持有或减仓", 85
        else:
            water, water_adv, water_risk = "极高位", "防止回撤", 95

        # ── 9段趋势阶段 ──
        if last < ma[20] < ma[55] and chg5 < 0 and (vp_lv == 0 or last < ma[120]):
            stage = "破位下跌"
        elif last < ma[20] and (not ma20_up or just_dead or not macd_gold):
            stage = "趋势转弱"
        elif vold >= 1.5 and abs(chg1) < 1 and pos52 > 65:
            stage = "放量滞涨"
        elif pos52 > 80 and abs(chg5) < 3 and not new_high_60:
            stage = "高位震荡"
        elif bull_align and new_high_60 and macd_gold:
            stage = "主升阶段"
        elif above[20] and above[55] and macd_gold and pos52 >= 45:
            stage = "趋势延续"
        elif above[20] and macd_gold and pos52 < 55:
            stage = "启动确认"
        elif above[20] and pos52 < 45 and volr > 1.05:
            stage = "底部启动"
        elif pos52 < 35 and not above[20]:
            stage = "底部试探"
        else:
            stage = "震荡整理"

        # ── 8项分数拆解(各0-100) ──
        s_price = _clamp(50 + chg20 * 1.6 + (12 if above[20] else -12) + (8 if above[55] else -8))
        s_vol = _clamp(50 + (volr - 1) * 120 * (1 if chg5 >= 0 else -1))
        s_macd = _clamp((70 if macd_gold else 30) + (15 if (red and hist_expand) else (-15 if (not red and hist_expand) else 0))
                        + (10 if just_cross else 0) + (10 if div.startswith("🌱") else (-10 if div.startswith("⚠️") else 0)))
        s_ma = _clamp(sum([above[5], above[10], above[20], above[55]]) * 22 + (12 if bull_align else 0))
        s_vph = {0: 20, 1: 55, 2: 90}[vp_lv]
        s_water = _clamp(100 - water_risk)          # 水位越高风险越大→分越低
        s_sector = float(sector_strength) if sector_strength is not None else 50.0

        # 资金动向：OBV 能量潮（涨日+量、跌日-量的累积），比"新闻催化"实在
        _sign = (c.diff() > 0).astype(int) * 2 - 1
        obv = (_sign * v).cumsum()
        obv5 = float(obv.iloc[-1] - obv.iloc[-6]) if len(obv) >= 6 else 0.0
        obv20 = float(obv.iloc[-1] - obv.iloc[-21]) if len(obv) >= 21 else 0.0
        # 连续流入/流出天数（OBV逐日方向的尾部连击）
        _obv_d = obv.diff().dropna()
        streak, streak_dir = 0, 0
        for _x in reversed(_obv_d.tail(10).tolist()):
            _d1 = 1 if _x > 0 else (-1 if _x < 0 else 0)
            if streak == 0 and _d1 != 0:
                streak_dir = _d1; streak = 1
            elif _d1 == streak_dir and _d1 != 0:
                streak += 1
            else:
                break
        s_capital = _clamp(50 + (25 if obv5 > 0 else -25) + (15 if obv20 > 0 else -15))
        _burst = "·爆发式放量" if volr >= 1.5 else ("·温和放量" if volr >= 1.1 else ("·量能萎缩" if volr < 0.85 else ""))
        cap_desc = (f"资金{'连续' + str(streak) + '天' if streak >= 2 else '今日'}"
                    f"{'流入' if streak_dir > 0 else '流出'}"
                    f"·5日净{'流入' if obv5 > 0 else '流出'}·20日净{'流入' if obv20 > 0 else '流出'}{_burst}")

        # 每一维的「实际情况」——分数只是结论，这里说清到底发生了什么
        _vol_pct = (volr - 1) * 100
        descs = {
            "价格趋势": f"20日{chg20:+.1f}%·5日{chg5:+.1f}%·{'站上' if above[20] else '跌破'}MA20",
            "均线结构": f"{ma_state}（{ma_txt}）",
            "MACD": macd_txt,
            "成交量": (("明显放量" if _vol_pct >= 20 else "温和放量" if _vol_pct >= 8 else
                     "明显缩量" if _vol_pct <= -20 else "温和缩量" if _vol_pct <= -8 else "量能持平")
                    + f"·5日均量较20日{_vol_pct:+.0f}%·今日量比{vold:.2f}"),
            "量价健康": vp,
            "水位风险": f"{water}({pos52:.0f}%)·距压力{(resistance / last - 1) * 100:+.1f}%·距支撑{(support / last - 1) * 100:+.1f}%",
            "板块强度": (f"板块资金轮入·热度{s_sector:.0f}" if (sector_strength is not None and s_sector >= 70)
                     else (f"板块涨势退潮·热度{s_sector:.0f}" if (sector_strength is not None and s_sector <= 30)
                           else ("所属板块中性" if sector_strength is not None else "未接板块数据·按中性50"))),
            "资金动向": cap_desc,
        }
        weights = {"价格趋势": (s_price, 0.20, descs["价格趋势"]),
                   "均线结构": (s_ma, 0.18, descs["均线结构"]),
                   "MACD": (s_macd, 0.15, descs["MACD"]),
                   "成交量": (s_vol, 0.12, descs["成交量"]),
                   "量价健康": (s_vph, 0.15, descs["量价健康"]),
                   "水位风险": (s_water, 0.10, descs["水位风险"]),
                   "板块强度": (s_sector, 0.05, descs["板块强度"]),
                   "资金动向": (s_capital, 0.05, descs["资金动向"])}
        total = int(round(sum(sc * w for sc, w, _ in weights.values())))

        # ── 8种动作 + 全价位 ──
        buy_lo, buy_hi = round(ma[20], 2), round(last, 2) if last > ma[20] else round(ma[20] * 1.02, 2)
        pullback = round(ma[10] if above[10] else ma[20], 2)
        breakout = round(max(h60, resistance), 2)
        stop = round(min(ma[55], l20) if stage not in ("底部试探", "底部启动") else l20, 2)
        reduce = round(resistance, 2)

        if stage == "破位下跌":
            action, concl = "🛑 趋势破坏，剔除/离场", "回避"
            invalid = f"重新站上MA20({ma[20]:.2f})并缩量企稳3日才重新评估"
        elif stage == "趋势转弱":
            action, concl = "🛑 跌破止损离场；空仓者回避", "回避"
            invalid = f"收复MA20({ma[20]:.2f})并放量收阳"
        elif stage in ("放量滞涨", "高位震荡"):
            action = "📉 冲高减仓/不追高" + (f"，接近{reduce:.2f}或放量滞涨减仓" if stage == "放量滞涨" else "")
            concl = "减仓"
            invalid = f"跌破MA20({ma[20]:.2f})即离场；缩量整理后放量破{breakout:.2f}方可回补"
        elif stage == "主升阶段":
            if bias20 > 8 or rsi > 75:
                action, concl = f"✋ 短线过热(乖离{bias20:+.1f}%/RSI{rsi:.0f})·不追，回踩MA10({pullback:.2f})再上", "持有"
            else:
                action, concl = f"🟢 持有；突破{breakout:.2f}放量可加仓", "进攻"
            invalid = f"收盘跌破MA20({ma[20]:.2f})且放量，主升结束"
        elif stage in ("趋势延续", "启动确认"):
            action = (f"🟢 可以买：{buy_lo:.2f}~{buy_hi:.2f}分批；突破{breakout:.2f}放量加仓" if vp_lv >= 1
                      else f"⏳ 等回踩MA20({ma[20]:.2f})企稳再买")
            concl = "进攻" if vp_lv >= 1 else "等待"
            invalid = f"收盘跌破MA55({ma[55]:.2f})趋势失效"
        elif stage in ("底部启动", "底部试探"):
            action, concl = f"🧪 只能试仓(≤半仓)，止损{l20:.2f}；站稳MA20放量再加", "试仓"
            invalid = f"跌回启动前低{l20:.2f}，启动失败"
        else:
            action, concl = f"⏳ 观望：站稳MA20({ma[20]:.2f})+放量再介入", "等待"
            invalid = "站上MA20且放量突破前高"


        # 【V100·用户定则】分数=现在值得买：时机差（减仓/回避/等待类结论）强制压分，
        # 高分从此代表"可以直接买"；持仓者的减仓提示由 action 单独说明。
        _timing_caps = {"回避": 45, "减仓": 58, "等待": 64, "试仓": 68}
        _tcap = _timing_caps.get(concl)
        if _tcap is not None and total > _tcap:
            total = _tcap

        return {
            "last": round(last, 2), "total": total, "stage": stage, "vp": vp, "vp_lv": vp_lv,
            "water": water, "water_adv": water_adv, "pos52": round(pos52),
            "macd_txt": macd_txt, "ma_txt": ma_txt, "ma_state": ma_state,
            "action": action, "conclusion": concl,
            "buy_zone": f"{buy_lo:.2f}~{buy_hi:.2f}", "pullback": pullback, "breakout": breakout,
            "stop": stop, "reduce": reduce, "invalid": invalid,
            "support": support, "resistance": resistance,
            "rsi": round(rsi), "bias20": round(bias20, 1), "volr": round(volr, 2),
            "chg5": round(chg5, 1), "chg20": round(chg20, 1),
            "breakdown": {k: (round(sc), w, d) for k, (sc, w, d) in weights.items()},
            "sector_known": sector_strength is not None,
            "ma": {k: round(x, 2) for k, x in ma.items()},
        }
    except Exception:
        return None


def horizon_scores(df, idx_close=None, full=None):
    """【V101】三期限「可买性」评分 —— 三端唯一实现（V88桌面/云端日报/轻量版共用）。
    设计定则（用户确立）：高分=现在值得买；每一分都有可复算的因子依据，逻辑链全透明。

    ┌ 短线(1-5日) ─ 动能30 + 相对强度RS20 25 + 量价确认20 + 时机位置15 + 均线形态10
    ├ 中线(1-3月) ─ 趋势结构30 + 引擎质量25 + 中期动量20 + RS60 15 + 量能持续10
    └ 长线(6月+)  ─ 长期结构30 + 引擎质量25 + 稳定性20 + RS120 15 + 价值位置10
    末端统一叠加 V100 时机闸门：减仓≤58 / 回避≤45 / 等待≤64 / 试仓≤68。

    参数：df=个股OHLCV；idx_close=对应大盘收盘序列(缺省按超额0算RS)；
         full=analyze_trend_full(df) 结果(传入则复用，不传内部计算)。
    返回 {"short"/"mid"/"long": {"score": int, "why": 逻辑链str},
          "gate": 引擎结论, "gate_note": 降分说明, "rs20": float, "chg20": float}
    """
    try:
        if full is None:
            full = analyze_trend_full(df)
        if not full or df is None or len(df) < 40:
            return None
        c = df["Close"].dropna()
        v = df["Volume"].fillna(0)
        last = float(c.iloc[-1])
        n = len(c)

        def _lin(x, lo, hi):
            return max(0.0, min(1.0, (x - lo) / (hi - lo))) if hi > lo else 0.0

        def _chg(days):
            return (last / float(c.iloc[-days - 1]) - 1) * 100 if n > days else 0.0

        chg5, chg20 = float(full.get("chg5", 0)), float(full.get("chg20", 0))
        chg60, chg120 = _chg(60), _chg(120)

        def _ichg(days):
            try:
                if idx_close is None or len(idx_close) <= days:
                    return 0.0
                return (float(idx_close.iloc[-1]) / float(idx_close.iloc[-days - 1]) - 1) * 100
            except Exception:
                return 0.0

        rs20, rs60, rs120 = chg20 - _ichg(20), chg60 - _ichg(60), chg120 - _ichg(120)

        ma = full["ma"]  # {5,10,20,55,120}
        above = {k: last > ma[k] for k in (5, 10, 20, 55)}
        ma200 = float(c.rolling(min(200, n)).mean().iloc[-1])
        ma200_prev = float(c.rolling(min(200, n)).mean().iloc[-21]) if n >= 221 else ma200
        ma20s = c.rolling(20).mean()
        ma20_up = n >= 25 and float(ma20s.iloc[-1]) > float(ma20s.iloc[-5])
        bull_align = last > ma[5] > ma[20] > ma[55]

        dif = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        dea = dif.ewm(span=9, adjust=False).mean()
        hist = dif - dea
        macd_gold = float(dif.iloc[-1]) > float(dea.iloc[-1])
        red = float(hist.iloc[-1]) > 0
        hist_expand = len(hist) >= 3 and abs(float(hist.iloc[-1])) > abs(float(hist.iloc[-3]))

        volr = float(full.get("volr", 1.0))
        _sign = (c.diff() > 0).astype(int) * 2 - 1
        obv = (_sign * v).cumsum()
        obv20 = float(obv.iloc[-1] - obv.iloc[-21]) if len(obv) >= 21 else 0.0
        vol60 = float(c.pct_change().tail(60).std() or 0) * 100
        _w = c.tail(min(120, n))
        dd120 = float(((_w.cummax() - _w) / _w.cummax()).max()) * 100 if len(_w) > 5 else 0.0

        rsi = float(full["rsi"]); bias20 = float(full["bias20"])
        pos52 = float(full["pos52"]); vp_lv = int(full["vp_lv"])
        room = (float(full.get("resistance", last)) / last - 1) * 100 if last else 0.0

        # ── ⚡ 短线(1-5日)：动能延续是第一性——强者恒强的持续期通常3-5日 ──
        s_mom = 8 * macd_gold + 6 * (red and hist_expand) + 10 * _lin(chg5, -2, 6) + 6 * (chg20 > 0)
        s_rs = 25 * _lin(rs20, -5, 12)
        s_vp = {2: 20, 1: 11, 0: 3}[vp_lv]
        s_pos = 6 * (1 - _lin(rsi, 68, 85)) + 6 * (1 - _lin(bias20, 5, 12)) + 3 * _lin(room, 1, 5)
        s_ma = 4 * above[5] + 3 * above[10] + 3 * above[20]
        short = s_mom + s_rs + s_vp + s_pos + s_ma
        why_s = (f"动能{s_mom:.0f}/30({'金叉' if macd_gold else '死叉'}"
                 f"{'·柱体扩大' if hist_expand and red else ''}·5日{chg5:+.1f}%)"
                 f" · RS{s_rs:.0f}/25(20日超额{rs20:+.1f}%)"
                 f" · 量价{s_vp}/20({str(full.get('vp','')).split('·')[0]})"
                 f" · 时机{s_pos:.0f}/15(RSI{rsi:.0f}·乖离{bias20:+.1f}%·距压力{room:+.1f}%)"
                 f" · 均线{s_ma:.0f}/10")

        # ── 🚀 中线(1-3月)：趋势结构>一切——多头排列+站稳季线的延续概率最高 ──
        m_tr = 12 * bull_align + 8 * above[55] + 5 * ma20_up + 5 * above[20]
        m_q = 25 * float(full["total"]) / 100.0
        m_mo = 12 * _lin(chg60, -5, 30) + 8 * _lin(chg20, -3, 15)
        m_rs = 15 * _lin(rs60, -8, 20)
        m_vol = 5 * (obv20 > 0) + 5 * (0.9 <= volr <= 2.0)
        mid = m_tr + m_q + m_mo + m_rs + m_vol
        why_m = (f"趋势{m_tr:.0f}/30({'多头排列' if bull_align else ('站上MA55' if above[55] else '结构未成')})"
                 f" · 质量{m_q:.0f}/25(引擎{full['total']})"
                 f" · 动量{m_mo:.0f}/20(60日{chg60:+.1f}%)"
                 f" · RS{m_rs:.0f}/15(60日超额{rs60:+.1f}%)"
                 f" · 量能{m_vol:.0f}/10({'OBV20日净流入' if obv20 > 0 else 'OBV净流出'}"
                 f"{'·温和放量' if 0.9 <= volr <= 2.0 else ''})")

        # ── 🏛 长线(6月+)：年线之上+低波动+长期跑赢大盘，买点还要位置不过热 ──
        l_st = 12 * (last > ma200) + 8 * (ma[55] > ma200) + 10 * (ma200 > ma200_prev)
        l_q = 25 * float(full["total"]) / 100.0
        l_sb = 12 * (1 - _lin(vol60, 1.2, 4.0)) + 8 * (1 - _lin(dd120, 15, 45))
        l_rs = 15 * _lin(rs120, -10, 30)
        l_ps = 10 * (1 - _lin(pos52, 75, 97))
        long_ = l_st + l_q + l_sb + l_rs + l_ps
        why_l = (f"结构{l_st:.0f}/30({'年线上方' if last > ma200 else '年线下方'}"
                 f"{'·年线上行' if ma200 > ma200_prev else ''})"
                 f" · 质量{l_q:.0f}/25"
                 f" · 稳定{l_sb:.0f}/20(日波动{vol60:.1f}%·120日回撤{dd120:.0f}%)"
                 f" · RS{l_rs:.0f}/15(120日超额{rs120:+.1f}%)"
                 f" · 位置{l_ps:.0f}/10(52周水位{pos52:.0f}%)")

        # ── V100 时机闸门（最终裁决：能不能现在买）──
        concl = str(full.get("conclusion", ""))
        cap = {"回避": 45, "减仓": 58, "等待": 64, "试仓": 68}.get(concl)
        gate_note = ""
        if cap is not None:
            if max(short, mid, long_) > cap:
                gate_note = f"⏳时机闸门：{full.get('stage', '')}·{concl}→三期限分上限{cap}"
            short, mid, long_ = min(short, cap), min(mid, cap), min(long_, cap)
        # 主升过热（乖离/RSI过高，引擎动作=✋不追）：短线分封顶72——短线不追高；
        # 中长线保留原分，回踩MA10/MA20即是买点
        elif concl == "持有" and "过热" in str(full.get("action", "")) and short > 72:
            short = 72.0
            gate_note = (gate_note + "；" if gate_note else "") + "✋主升过热→短线分上限72(等回踩)"

        return {"short": {"score": int(round(short)), "why": why_s},
                "mid": {"score": int(round(mid)), "why": why_m},
                "long": {"score": int(round(long_)), "why": why_l},
                "gate": concl, "gate_note": gate_note,
                "rs20": round(rs20, 1), "chg20": round(chg20, 1)}
    except Exception:
        return None


# 旧接口兼容：trend_pulse 返回精简子集，避免其它调用处报错
def trend_pulse(df):
    r = analyze_trend_full(df)
    if not r:
        return None
    return {"last": r["last"], "score": r["total"], "stage": r["stage"], "vp": r["vp"],
            "action": r["action"], "support": r["support"], "resistance": r["resistance"],
            "invalid": r["invalid"], "reasons": [], "rsi": r["rsi"], "bias20": r["bias20"],
            "volr": r["volr"], "chg5": r["chg5"], "chg20": r["chg20"], "pos52": r["pos52"],
            "ma": r["ma"]}



def analyze(code: str) -> dict:
    """搜索入口：代码/中文名/拼音 → {symbol, full} 或 {error}。只解析一次(含东财搜索)。"""
    raw = str(code).strip()
    sym = to_yf(raw)  # 单次解析：内置映射→代码归一→东财搜索
    # 若仍是中文/无法解析（东财也没搜到）→ 友好提示
    if any("一" <= ch <= "鿿" for ch in sym):
        return {"error": f"没找到「{raw}」。可换个说法(全称/简称)或直接用代码：美股字母(AAPL)、港股数字(0700)、A股6位(600519)。"}
    df = fetch(sym)
    if df is None:
        return {"error": f"未取到 {sym} 的行情（代码可能有误，或该股云端暂时取不到数据，A股偶发，可稍后重试）"}
    r = analyze_trend_full(df)
    if not r:
        return {"error": "数据不足，无法计算"}
    return {"symbol": sym, "full": r, "asof": str(df.index[-1])[:10]}
