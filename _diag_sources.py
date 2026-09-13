"""V88 数据源全链路诊断 —— 逐源验证，定位行情 N/A 根因。一次性脚本。"""
import os, sys, time, json

def line(): print("-" * 70)

# ── 1. 东方财富：带代理 vs 不带代理 对比（这是当前最大嫌疑）─────────────
def test_eastmoney():
    import requests
    base = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    samples = {
        "上证指数 1.000001": "1.000001",
        "SPY 107.SPY": "107.SPY",
        "VIX 100.VIX": "100.VIX",
        "QQQ 105.QQQ": "105.QQQ",
        "10Y 100.UST10Y": "100.UST10Y",
        "DXY 100.UDI": "100.UDI",
    }
    for label, mode_proxy in [("直连(无代理)", None), ("走Clash代理", "http://127.0.0.1:7897")]:
        print(f"\n[东财 · {label}]")
        s = requests.Session()
        s.trust_env = False
        if mode_proxy:
            s.proxies = {"http": mode_proxy, "https": mode_proxy}
        for name, secid in samples.items():
            try:
                r = s.get(base, params={
                    'secid': secid, 'fields1': 'f1,f2,f3,f4,f5,f6',
                    'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
                    'klt': '101', 'fqt': '0' if secid.startswith('100.') or secid.startswith('1.') else '1',
                    'end': '20500101', 'lmt': '60',
                }, timeout=6)
                d = r.json()
                kl = (d.get('data') or {}).get('klines') if d.get('data') else None
                n = len(kl) if kl else 0
                last = kl[-1].split(',')[2] if n else '-'
                print(f"   {name:18s} HTTP{r.status_code} rows={n:3d} last={last}")
            except Exception as e:
                print(f"   {name:18s} ERROR {type(e).__name__}: {str(e)[:60]}")

# ── 2. yfinance 1.4.1（美股，带代理，复刻 app 调用）──────────────────────
def test_yfinance():
    os.environ["http_proxy"] = "http://127.0.0.1:7897"
    os.environ["https_proxy"] = "http://127.0.0.1:7897"
    import yfinance as yf
    print(f"\n[yfinance {yf.__version__} · 带代理]")
    for sym in ["SPY", "^VIX", "QQQ", "GLD", "TLT", "^TNX", "DX-Y.NYB"]:
        try:
            t = yf.Ticker(sym)
            df = t.history(period="6mo")
            n = 0 if df is None else len(df)
            last = round(float(df['Close'].iloc[-1]), 2) if n else None
            print(f"   {sym:12s} rows={n:3d} last={last}")
        except Exception as e:
            print(f"   {sym:12s} ERROR {type(e).__name__}: {str(e)[:70]}")

# ── 3. yfinance 不带代理（直连，看美股是否其实直连更稳）─────────────────
def test_yfinance_direct():
    for k in ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"]:
        os.environ.pop(k, None)
    import importlib, yfinance as yf
    print(f"\n[yfinance {yf.__version__} · 直连无代理]")
    for sym in ["SPY", "^VIX", "QQQ"]:
        try:
            df = yf.Ticker(sym).history(period="1mo")
            n = 0 if df is None else len(df)
            print(f"   {sym:12s} rows={n:3d} last={round(float(df['Close'].iloc[-1]),2) if n else None}")
        except Exception as e:
            print(f"   {sym:12s} ERROR {type(e).__name__}: {str(e)[:70]}")

# ── 4. GPT-6 Codex订阅连通性（日报引擎）─────────────────────────────────
def test_gpt_subscription():
    print("\n[GPT-6 Codex订阅 / GPT-6 Astra]")
    try:
        from desktop_gpt_subscription import complete
        t0 = time.time()
        text, body = complete("只回复两个字：正常", max_tokens=32, reasoning_effort="low", timeout=30)
        print(f"   ✅ 返回: {text!r}  ({(time.time()-t0):.1f}s)  模型={body.get('model')}")
    except Exception as e:
        print(f"   ❌ ERROR {type(e).__name__}: {str(e)[:120]}")

# ── 5. Tushare A股（如配置 token）──────────────────────────────────────
def test_tushare():
    print("\n[旧Tushare入口已停用，改测免费核验日线]")
    from market_data_helper import fetch_daily_free
    frame = fetch_daily_free("000001.SZ")
    print("合格日线根数:", len(frame) if frame is not None else 0)

if __name__ == "__main__":
    print("=" * 70); print("V88 数据源全链路诊断"); print("=" * 70)
    tests = sys.argv[1:] or ["em", "yf", "yfd", "gpt", "ts"]
    if "em" in tests: line(); test_eastmoney()
    if "yf" in tests: line(); test_yfinance()
    if "yfd" in tests: line(); test_yfinance_direct()
    if "gpt" in tests: line(); test_gpt_subscription()
    if "ts" in tests: line(); test_tushare()
    print("\n" + "=" * 70 + "\n诊断完成")
