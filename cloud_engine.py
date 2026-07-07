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
    "中国移动": "600941.SS", "工商银行": "601398.SS", "中国石油": "601857.SS", "中国神华": "601088.SS",
    # 美股
    "苹果": "AAPL", "英伟达": "NVDA", "微软": "MSFT", "谷歌": "GOOG", "亚马逊": "AMZN",
    "特斯拉": "TSLA", "台积电": "TSM", "meta": "META", "脸书": "META", "奈飞": "NFLX",
    "美光": "MU", "博通": "AVGO", "超微": "AMD", "英特尔": "INTC", "甲骨文": "ORCL",
    "强生": "JNJ", "礼来": "LLY", "默沙东": "MRK", "辉瑞": "PFE", "可口可乐": "KO",
    "摩根大通": "JPM", "伯克希尔": "BRK-B", "spacex": "SPCX", "网易": "NTES", "拼多多": "PDD",
}


def to_yf(code: str) -> str:
    """归一化：中文名/拼音→代码；AAPL→AAPL｜0700→0700.HK｜600519→600519.SS｜000001→000001.SZ"""
    raw = str(code).strip()
    # 简搜：先查中文名/别名映射（不区分大小写）
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
    return c


def is_chinese_name(code: str) -> bool:
    """判断输入是否为无法识别的中文名（用于给友好提示）"""
    raw = str(code).strip()
    return any("一" <= ch <= "鿿" for ch in raw) and raw not in NAME_MAP


def fetch(symbol: str):
    import yfinance as yf
    try:
        df = yf.Ticker(symbol).history(period="6mo")
        return df if df is not None and len(df) >= 30 else None
    except Exception:
        return None


def trend_pulse(df: pd.DataFrame) -> dict | None:
    """与桌面版 analyze_trend_pulse 同口径的精简版。"""
    try:
        if df is None or len(df) < 30:
            return None
        c = df["Close"].dropna()
        v = df["Volume"].fillna(0)
        hi, lo = df["High"], df["Low"]
        last = float(c.iloc[-1])
        ma = {n: float(c.rolling(min(n, len(c))).mean().iloc[-1]) for n in (5, 10, 20, 55, 120)}
        ma20_up = float(c.rolling(20).mean().iloc[-1]) > float(c.rolling(20).mean().iloc[-5]) if len(c) >= 25 else True

        dif = c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean()
        dea = dif.ewm(span=9, adjust=False).mean()
        hist = dif - dea
        macd_gold = float(dif.iloc[-1]) > float(dea.iloc[-1])
        hist_rising = len(hist) >= 3 and float(hist.iloc[-1]) > float(hist.iloc[-3])

        delta = c.diff()
        rs_ = delta.clip(lower=0).ewm(com=13).mean() / (-delta.clip(upper=0)).ewm(com=13).mean()
        rsi = float((100 - 100 / (1 + rs_)).iloc[-1])

        v5, v20 = float(v.tail(5).mean()), float(v.tail(20).mean()) or 1.0
        volr = v5 / v20
        chg5 = (last / float(c.iloc[-6]) - 1) * 100 if len(c) >= 6 else 0.0
        chg20 = (last / float(c.iloc[-21]) - 1) * 100 if len(c) >= 21 else 0.0
        bias20 = (last / ma[20] - 1) * 100 if ma[20] else 0.0

        h60 = float(hi.tail(60).max())
        l20 = float(lo.tail(20).min())
        l250 = float(lo.tail(min(250, len(lo))).min())
        h250 = float(hi.tail(min(250, len(hi))).max())
        pos52 = (last - l250) / (h250 - l250) * 100 if h250 > l250 else 50.0
        new_high_60 = float(hi.iloc[-1]) >= h60 * 0.995
        support = max(ma[20], l20) if last > ma[20] else max(ma[55], l20)
        resistance = h60 if last < h60 * 0.99 else h250

        reasons = []
        if chg5 > 1.5 and volr >= 1.1:
            vp, vpg = "📈 放量上涨·量价健康", 2
            reasons.append(f"5日+{chg5:.1f}%且量比{volr:.2f}放大，资金进场")
        elif chg5 > 1.5 and volr < 0.85:
            vp, vpg = "⚠️ 缩量上涨·上攻乏力", 1
            reasons.append(f"上涨但量比仅{volr:.2f}，追高动能存疑")
        elif chg5 < -1.5 and volr < 0.9:
            vp, vpg = "🔄 缩量回调·抛压有限", 1
            reasons.append(f"回调{chg5:.1f}%但缩量({volr:.2f})，正常回踩概率大")
        elif chg5 < -1.5 and volr >= 1.2:
            vp, vpg = "🚨 放量下跌·出货嫌疑", 0
            reasons.append(f"下跌{chg5:.1f}%且放量({volr:.2f})，主动抛压明显")
        elif volr >= 1.5 and abs(chg5) < 1.5:
            vp, vpg = "⚠️ 放量滞涨·分歧加大", 0
            reasons.append(f"量比{volr:.2f}放大但价格滞涨，多空分歧")
        else:
            vp, vpg = "➖ 量价中性", 1

        if last < ma[20] < ma[55] and chg5 < 0 and (vpg == 0 or last < ma[120]):
            stage = "🔴 破位下跌"
            reasons.append(f"价({last:.2f})<MA20({ma[20]:.2f})<MA55，均线空头")
        elif last < ma[20] and (not ma20_up or not macd_gold):
            stage = "🟠 趋势转弱"
            reasons.append(f"跌破MA20({ma[20]:.2f})" + ("且MACD死叉" if not macd_gold else "，MA20走平向下"))
        elif volr >= 1.5 and abs(chg5) < 1.5 and pos52 > 70:
            stage = "🟡 放量滞涨"
        elif pos52 > 80 and abs(chg5) < 3 and not new_high_60:
            stage = "🟡 高位震荡"
            reasons.append(f"52周高位({pos52:.0f}%)横盘，未创新高")
        elif last > ma[5] > ma[20] and new_high_60 and macd_gold:
            stage = "🚀 主升阶段"
            reasons.append("多头排列+创60日新高+MACD金叉")
        elif last > ma[20] > ma[55] and macd_gold:
            stage = "🟢 趋势确认"
            reasons.append("站稳MA20/MA55多头排列，MACD金叉")
        elif last > ma[20] and pos52 < 45 and volr > 1.05:
            stage = "🌱 底部启动"
            reasons.append(f"低位({pos52:.0f}%)放量站上MA20")
        else:
            stage = "➖ 震荡整理"

        if stage == "🔴 破位下跌":
            action = "🛑 趋势破坏，剔除/离场"
            invalid = f"重新站上MA20({ma[20]:.2f})且缩量企稳3日"
        elif stage == "🟠 趋势转弱":
            action = "🛑 持有者跌破止损离场；空仓者回避"
            invalid = f"收复MA20({ma[20]:.2f})并放量收阳"
        elif stage == "🟡 放量滞涨":
            action = "📉 冲高减仓（先落袋一部分）"
            invalid = f"缩量整理后再放量突破{resistance:.2f}"
        elif stage == "🟡 高位震荡":
            action = "✋ 不追高；持有可持有但设好止损"
            invalid = f"跌破MA20({ma[20]:.2f})即减仓"
        elif stage == "🚀 主升阶段":
            if bias20 > 8 or rsi > 75:
                action = f"✋ 短线过热(乖离{bias20:+.1f}%/RSI{rsi:.0f})·不追，等回踩MA10({ma[10]:.2f})"
                reasons.append(f"乖离{bias20:+.1f}%、RSI{rsi:.0f}，短线透支")
            else:
                action = f"🟢 继续持有；新买回踩MA10({ma[10]:.2f})分批"
            invalid = f"收盘跌破MA20({ma[20]:.2f})且放量"
        elif stage == "🟢 趋势确认":
            action = (f"🟢 可以买：{ma[20]:.2f}~{last:.2f}分批" if vpg >= 1
                      else f"⏳ 等回踩MA20({ma[20]:.2f})企稳再买")
            invalid = f"收盘跌破MA55({ma[55]:.2f})"
        elif stage == "🌱 底部启动":
            action = f"🧪 只能试仓(≤半仓)，止损{l20:.2f}"
            invalid = f"跌回启动前低点{l20:.2f}"
        else:
            action = f"⏳ 观望：站稳MA20({ma[20]:.2f})+放量再介入"
            invalid = "—"

        align = sum([last > ma[5], ma[5] > ma[20], ma[20] > ma[55], last > ma[120]])
        score = int(max(0, min(100, align * 7.5 + (10 if macd_gold else 0) + (10 if hist_rising else 0)
                               + vpg * 10 + max(0, min(15, 7.5 + chg20 * 0.75))
                               + (15 if (45 <= rsi <= 70 and abs(bias20) < 8) else (7 if rsi < 80 else 0)))))
        return {"last": round(last, 2), "score": score, "stage": stage, "vp": vp, "action": action,
                "support": round(support, 2), "resistance": round(resistance, 2), "invalid": invalid,
                "reasons": reasons[:4], "rsi": round(rsi), "bias20": round(bias20, 1), "volr": round(volr, 2),
                "chg5": round(chg5, 1), "chg20": round(chg20, 1), "pos52": round(pos52),
                "ma": {k: round(x, 2) for k, x in ma.items()}}
    except Exception:
        return None


def analyze(code: str) -> dict:
    """搜索入口：代码/中文名 → {symbol, tp} 或 {error}"""
    if is_chinese_name(code):
        return {"error": f"没收录「{code}」这个名字。请改用代码搜索：美股用字母(如 AAPL)、"
                         f"港股用数字(如 0700)、A股用6位数字(如 600519)。常用票也可直接打名字，如 腾讯/茅台/英伟达。"}
    sym = to_yf(code)
    df = fetch(sym)
    if df is None:
        return {"error": f"未取到 {sym} 的行情（代码可能有误，或该股云端暂时取不到数据，A股偶发，可稍后重试）"}
    tp = trend_pulse(df)
    if not tp:
        return {"error": "数据不足，无法计算"}
    return {"symbol": sym, "tp": tp, "asof": str(df.index[-1])[:10]}
